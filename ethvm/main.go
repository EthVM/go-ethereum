// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethvm

import (
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	r "gopkg.in/gorethink/gorethink.v3"
	"gopkg.in/urfave/cli.v1"
)

var (
	// EthVMFlag Save blockchain data to external db, make sure to set RETHINKDB_URL env variable
	EthVMFlag = cli.BoolFlag{
		Name:  "ethvm",
		Usage: "Save blockchain data to external db, make sure to set RETHINKDB_URL env variable",
	}

	// EthVMCertFlag Use custom ssl cert for rethinkdb connection, make sure to set RETHINKDB_CERT env variable
	EthVMCertFlag = cli.BoolFlag{
		Name:  "ethvm.cert",
		Usage: "Use custom ssl cert for rethinkdb connection, make sure to set RETHINKDB_CERT env variable",
	}

	// EthVMDbNameFlag Select which name will be asigned to the RethinkDB db table
	EthVMDbNameFlag = cli.StringFlag{
		Name:  "ethvm.dbName",
		Usage: "Select which name will be asigned to the RethinkDB db table",
	}

	// DbTables DB Metadata
	DbTables = map[string]string{
		"blocks":       "blocks",
		"block_stats":  "block_stats",
		"transactions": "transactions",
		"traces":       "traces",
		"logs":         "logs",
		"data":         "data",
	}

	// TraceStr Javascript definition for the tracer that analyzes transactions
	TraceStr = "{transfers:[],isError:false,msg:'',result:function(){var _this=this;return{transfers:_this.transfers,isError:_this.isError,msg:_this.msg}},step:function(log,db){var _this=this;if(log.err){_this.isError=true;_this.msg=log.err.Error();return}var op=log.op;var stack=log.stack;var memory=log.memory;var transfer={};var from=log.account;if(op.toString()=='CALL'){transfer={op:'CALL',value:stack.peek(2).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.BigToAddress(stack.peek(1)),toBalance:db.getBalance(big.BigToAddress(stack.peek(1))).Bytes(),input:memory.slice(big.ToInt(stack.peek(3)),big.ToInt(stack.peek(3))+big.ToInt(stack.peek(4)))};_this.transfers.push(transfer)}else if(op.toString()=='SELFDESTRUCT'){transfer={op:'SELFDESTRUCT',value:db.getBalance(from).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.BigToAddress(stack.peek(0)),toBalance:db.getBalance(big.BigToAddress(stack.peek(0))).Bytes()};_this.transfers.push(transfer)}else if(op.toString()=='CREATE'){transfer={op:'CREATE',value:stack.peek(0).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.CreateContractAddress(from,db.getNonce(from)),toBalance:db.getBalance(big.CreateContractAddress(from,db.getNonce(from))).Bytes()input:memory.slice(big.ToInt(stack.peek(1)),big.ToInt(stack.peek(1))+big.ToInt(stack.peek(2)))};_this.transfers.push(transfer)}}}"

	// Block rewards
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)

	// Meta values
	ctx *cli.Context

	// Global EthVM instance
	instance *EthVM
)

// ------------------
// EthVM data structs
// ------------------

type BlockTx struct {
	Tx        *types.Transaction
	Trace     interface{}
	Pending   bool
	Timestamp *big.Int
}

type BlockIn struct {
	Block           *types.Block
	BlockTxs        *[]BlockTx
	State           *state.StateDB
	PrevTd          *big.Int
	Receipts        types.Receipts
	Signer          types.Signer
	IsUncle         bool
	TxFees          *big.Int
	BlockRewardFunc func(block *types.Block) (*big.Int, *big.Int)
	UncleRewardFunc func(uncles []*types.Header, index int) *big.Int
	UncleReward     *big.Int
}

// NewBlockIn Creates and formats a new BlockIn instance
func NewBlockIn(block *types.Block, blockTxs *[]BlockTx, state *state.StateDB, td *big.Int, receipts []*types.Receipt, signer types.Signer, txFees *big.Int, blockReward *big.Int) *BlockIn {
	return &BlockIn{
		Block:    block,
		BlockTxs: blockTxs,
		State:    state,
		PrevTd:   td,
		Receipts: receipts,
		Signer:   signer,
		IsUncle:  false,
		TxFees:   txFees,
		BlockRewardFunc: func(block *types.Block) (*big.Int, *big.Int) {
			reward := new(big.Int).Set(blockReward)
			multiplier := new(big.Int).Div(blockReward, big32)
			uncleReward := new(big.Int).Mul(multiplier, big.NewInt(int64(len(block.Uncles()))))
			return reward, uncleReward
		},
		UncleRewardFunc: func(uncles []*types.Header, index int) *big.Int {
			reward := new(big.Int)
			for i, uncle := range uncles {
				reward.Add(uncle.Number, big8)
				reward.Sub(reward, block.Header().Number)
				reward.Mul(reward, blockReward)
				reward.Div(reward, big8)
				if i == index {
					return reward
				}
			}
			return big.NewInt(0)
		},
	}
}

type TXMetric struct {
	status   uint
	pending  bool
	gasPrice *big.Int
	gasUsed  *big.Int
	rf       interface{}
	nonce    uint64
	to       *common.Address
	from     *common.Address
}

func newTxMetric(blockIn *BlockIn, txBlock BlockTx, index int) TXMetric {
	tx := txBlock.Tx
	receipt := blockIn.Receipts[index]

	// if no receipt, then there is no metrics to be calculated
	if receipt == nil {
		log.Debug("newTxMetric - Receipt not found for transaction", "hash", tx.Hash())
		return TXMetric{}
	}

	signer := blockIn.Signer
	from, _ := types.Sender(signer, tx)

	return TXMetric{
		gasPrice: tx.GasPrice(),
		gasUsed:  big.NewInt(int64(receipt.GasUsed)),
		pending:  txBlock.Pending,
		status:   uint(receipt.Status),
		nonce:    tx.Nonce(),
		to:       tx.To(),
		from:     &from,
	}
}

type BlockStats struct {
	totalGasUsed  *big.Int
	totalGasPrice *big.Int
	avgGasUsed    *big.Int
	avgGasPrice   *big.Int
	avgTxFees     *big.Int
	accounts      []*common.Address
	newAccounts   []*common.Address
	totalTxs      uint
	successfulTxs uint
	failedTxs     uint
	pendingTxs    uint
}

func newBlockStats(blockIn *BlockIn) BlockStats {
	bm := BlockStats{
		totalGasUsed:  big.NewInt(0),
		totalGasPrice: big.NewInt(0),
		avgGasUsed:    big.NewInt(0),
		avgGasPrice:   big.NewInt(0),
		avgTxFees:     big.NewInt(0),
		totalTxs:      0,
		successfulTxs: 0,
		failedTxs:     0,
		pendingTxs:    0,
	}

	if blockIn.BlockTxs == nil || blockIn.IsUncle {
		return bm
	}

	for i, block := range *blockIn.BlockTxs {
		bm.totalTxs++

		ttx := newTxMetric(blockIn, block, i)
		if ttx.status == uint(types.ReceiptStatusSuccessful) {
			bm.successfulTxs++
		}
		if ttx.status == uint(types.ReceiptStatusFailed) {
			bm.failedTxs++
		}
		if ttx.pending {
			bm.pendingTxs++
		}

		bm.accounts = append(bm.accounts, ttx.to)
		bm.accounts = append(bm.accounts, ttx.from)
		if ttx.nonce == 0 {
			bm.newAccounts = append(bm.newAccounts, ttx.from)
		}

		bm.totalGasPrice.Add(ttx.gasPrice, bm.totalGasPrice)
		bm.totalGasUsed.Add(ttx.gasUsed, bm.totalGasUsed)
	}

	if len(*blockIn.BlockTxs) > 0 {
		avgGasPrice := bm.totalGasPrice.Div(bm.totalGasPrice, big.NewInt(int64(len(*blockIn.BlockTxs))))
		bm.avgGasPrice = avgGasPrice
	}

	return bm
}

type PendingTx struct {
	Tx      *types.Transaction
	Trace   interface{}
	State   *state.StateDB
	Signer  types.Signer
	Receipt *types.Receipt
	Block   *types.Block
}

// -----------------
// Main EthVM struct
// -----------------

// EthVM Struct that holds metadata related to EthVM
type EthVM struct {
	enabled bool

	// RethinkDB
	session *r.Session
	dbName  string
}

// Init Saves cli.Context to be used inside EthVM
func Init(c *cli.Context) {
	ctx = c
}

// GetInstance Creates or return an EthVM instance (not thread safe!)
func GetInstance() *EthVM {
	if instance != nil {
		return instance
	}

	instance = &EthVM{
		enabled: ctx.GlobalBool(EthVMFlag.Name),
	}
	return instance
}

func (e *EthVM) isEnabled() bool {
	return e.enabled
}

func (e *EthVM) isConnected() bool {
	return e.isEnabled() && e.session != nil && e.session.IsConnected()
}

// Connect Performs connection to the DB (and creates tables and indices if needed)
func (e *EthVM) Connect() {
	if !e.isEnabled() || e.session != nil {
		return
	}

	var (
		opts *r.ConnectOpts
		err  error
	)

	// Read RETHINKDB_URL env variable
	if ctx.GlobalBool(EthVMFlag.Name) {
		rethinkurl, err := url.Parse(os.Getenv("RETHINKDB_URL"))
		if err != nil {
			panic("Could not parse correctly env RETHINKDB_URL!")
		}

		if rethinkurl.User != nil {
			password, isSet := rethinkurl.User.Password()
			if !isSet {
				panic("Password not especified correctly in $RETHINKDB_URL")
			}
			opts = &r.ConnectOpts{
				Address:  rethinkurl.Host,
				Username: rethinkurl.User.Username(),
				Password: password,
			}
		} else {
			opts = &r.ConnectOpts{
				Address: rethinkurl.Host,
			}
		}
	}

	// Double check that variable has been read correctly
	if opts == nil {
		panic("Could not parse correctly env RETHINKDB_URL!")
	}

	// Read RETHINKDB_CERT_RAW env variable if specified
	if ctx.GlobalBool(EthVMCertFlag.Name) {
		cert := os.Getenv("RETHINKDB_CERT_RAW")
		if cert == "" {
			panic("$RETHINKDB_CERT_RAW value is empty or invalid")
		}

		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM([]byte(cert))

		opts.TLSConfig = &tls.Config{
			RootCAs: roots,
		}
	}

	// Connect
	e.session, err = r.Connect(*opts)
	if err != nil {
		panic(err)
	}

	e.dbName = "eth_mainnet"
	if ctx.GlobalString(EthVMDbNameFlag.Name) != "" {
		e.dbName = ctx.GlobalString(EthVMDbNameFlag.Name)
	}

	// Create DB
	r.DBCreate(e.dbName).RunWrite(e.session)

	// Create tables
	for _, v := range DbTables {
		r.DB(e.dbName).TableCreate(v, r.TableCreateOpts{
			PrimaryKey: "hash",
		}).RunWrite(e.session)
	}

	// Setup indices
	r.DB(e.dbName).Table(DbTables["data"]).Insert(map[string]interface{}{
		"hash":       "cached",
		"pendingTxs": 0,
	}).RunWrite(e.session)

	r.DB(e.dbName).Table(DbTables["transactions"]).IndexCreate("nonceHash").RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["transactions"]).IndexCreate("cofrom", r.IndexCreateOpts{Multi: true}).RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["transactions"]).IndexCreate("to").RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["transactions"]).IndexCreate("from").RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["transactions"]).IndexCreateFunc("numberAndHash",
		[]interface{}{
			r.Row.Field("blockIntNumber"),
			r.Row.Field("hash"),
		}).RunWrite(e.session)

	r.DB(e.dbName).Table(DbTables["blocks"]).IndexCreate("intNumber").RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["block_stats"]).IndexCreate("timestamp").RunWrite(e.session)

	r.DB(e.dbName).Table(DbTables["traces"]).IndexCreateFunc("trace_from", r.Row.Field("trace").Field("transfers").Field("from"), r.IndexCreateOpts{Multi: true}).RunWrite(e.session)
	r.DB(e.dbName).Table(DbTables["traces"]).IndexCreateFunc("trace_to", r.Row.Field("trace").Field("transfers").Field("to"), r.IndexCreateOpts{Multi: true}).RunWrite(e.session)
}

// InsertGenesisTrace Adds a Genesis Block to the DB
func (e *EthVM) InsertGenesisTrace(gAlloc map[common.Address][]byte, block *types.Block) {
	if !e.isEnabled() {
		return
	}

	// Format
	rTrace := map[string]interface{}{
		"hash":           common.BytesToHash([]byte("GENESIS_TX")).Bytes(),
		"blockHash":      block.Hash().Bytes(),
		"blockNumber":    block.Header().Number.Bytes(),
		"blockIntNumber": hexutil.Uint64(block.Header().Number.Uint64()),
		"trace": map[string]interface{}{
			"isError": false,
			"msg":     "",
			"transfers": func() interface{} {
				var dTraces []interface{}
				for addr, balance := range gAlloc {
					dTraces = append(dTraces, map[string]interface{}{
						"op":    "BLOCK",
						"value": balance,
						"to":    addr.Bytes(),
						"type":  "GENESIS",
					})
				}
				dTraces = append(dTraces, map[string]interface{}{
					"op":          "BLOCK",
					"txFees":      big.NewInt(0).Bytes(),
					"blockReward": big.NewInt(5e+18).Bytes(),
					"uncleReward": big.NewInt(0).Bytes(),
					"to":          common.BytesToAddress(make([]byte, 1)).Bytes(),
					"type":        "REWARD",
				})
				return dTraces
			}(),
		},
	}

	// Save to DB
	_, err := r.DB(e.dbName).Table(DbTables["traces"]).Insert(rTrace, r.InsertOpts{
		Conflict: "replace",
	}).RunWrite(e.session)

	if err != nil {
		panic(err)
	}
}

// InsertBlock Adds a new Block to the DB
func (e *EthVM) InsertBlock(blockIn *BlockIn) {
	if !e.isEnabled() {
		return
	}

	processTxs := func(txblocks *[]BlockTx) ([][]byte, []interface{}, []interface{}, []interface{}) {
		var (
			tHashes [][]byte
			tTxs    []interface{}
			tLogs   []interface{}
			tTrace  []interface{}
		)
		if txblocks == nil {
			log.Info("InsertBlock - processTxs / Empty txblocks")
			return tHashes, tTxs, tLogs, tTrace
		}

		for i, _txBlock := range *txblocks {
			_tTx, _tLogs, _tTrace := formatTx(blockIn, _txBlock, i)
			tTxs = append(tTxs, _tTx)
			if _tLogs["logs"] != nil {
				tLogs = append(tLogs, _tLogs)
			}
			if _tTrace["trace"] != nil {
				tTrace = append(tTrace, _tTrace)
			}
			tHashes = append(tHashes, _txBlock.Tx.Hash().Bytes())
		}

		log.Info("InsertBlock - processTxs", "tHashes", len(tHashes), "tTxs", len(tTxs), "tLogs", len(tLogs), "tTrace", len(tTrace))
		return tHashes, tTxs, tLogs, tTrace
	}

	formatBlock := func(block *types.Block, tHashes [][]byte) (map[string]interface{}, error) {
		head := block.Header() // copies the header once
		minerBalance := blockIn.State.GetBalance(head.Coinbase)

		txFees, blockReward, uncleReward := func() ([]byte, []byte, []byte) {
			var (
				_txfees []byte
				_uncleR []byte
				_blockR []byte
			)
			if blockIn.TxFees != nil {
				_txfees = blockIn.TxFees.Bytes()
			} else {
				_txfees = make([]byte, 0)
			}
			if blockIn.IsUncle {
				_blockR = blockIn.UncleReward.Bytes()
				_uncleR = make([]byte, 0)
			} else {
				blockR, uncleR := blockIn.BlockRewardFunc(block)
				_blockR, _uncleR = blockR.Bytes(), uncleR.Bytes()

			}
			return _txfees, _blockR, _uncleR
		}()

		bfields := map[string]interface{}{
			"number":       head.Number.Bytes(),
			"intNumber":    hexutil.Uint64(head.Number.Uint64()),
			"hash":         head.Hash().Bytes(),
			"parentHash":   head.ParentHash.Bytes(),
			"nonce":        head.Nonce,
			"mixHash":      head.MixDigest.Bytes(),
			"sha3Uncles":   head.UncleHash.Bytes(),
			"logsBloom":    head.Bloom.Bytes(),
			"stateRoot":    head.Root.Bytes(),
			"miner":        head.Coinbase.Bytes(),
			"minerBalance": minerBalance.Bytes(),
			"difficulty":   head.Difficulty.Bytes(),
			"totalDifficulty": func() []byte {
				if blockIn.PrevTd == nil {
					return make([]byte, 0)
				}
				return (new(big.Int).Add(block.Difficulty(), blockIn.PrevTd)).Bytes()
			}(),
			"extraData":         head.Extra,
			"size":              big.NewInt(int64(hexutil.Uint64(block.Size()))).Bytes(),
			"gasLimit":          big.NewInt(int64(head.GasLimit)).Bytes(),
			"gasUsed":           big.NewInt(int64(head.GasUsed)).Bytes(),
			"timestamp":         head.Time.Bytes(),
			"transactionsRoot":  head.TxHash.Bytes(),
			"receiptsRoot":      head.ReceiptHash.Bytes(),
			"transactionHashes": tHashes,
			"uncleHashes": func() [][]byte {
				uncles := make([][]byte, len(block.Uncles()))
				for i, uncle := range block.Uncles() {
					uncles[i] = uncle.Hash().Bytes()
					e.InsertBlock(&BlockIn{
						Block:       types.NewBlockWithHeader(uncle),
						State:       blockIn.State,
						IsUncle:     true,
						UncleReward: blockIn.UncleRewardFunc(block.Uncles(), i),
					})
				}
				return uncles
			}(),
			"isUncle":     blockIn.IsUncle,
			"txFees":      txFees,
			"blockReward": blockReward,
			"uncleReward": uncleReward,
		}
		return bfields, nil
	}

	tHashes, tTxs, tLogs, tTrace := processTxs(blockIn.BlockTxs)
	bm := newBlockStats(blockIn)
	block, _ := formatBlock(blockIn.Block, tHashes)
	blockStats, _ := formatBlockStats(blockIn, blockIn.Block, bm)

	if block["intNumber"] != 0 {
		tTrace = append(tTrace, map[string]interface{}{
			"hash":           block["hash"],
			"blockHash":      block["hash"],
			"blockNumber":    block["number"],
			"blockIntNumber": block["intNumber"],
			"trace": map[string]interface{}{
				"isError": false,
				"msg":     "",
				"transfers": func() interface{} {
					var dTraces []interface{}
					dTraces = append(dTraces, map[string]interface{}{
						"op":          "BLOCK",
						"txFees":      block["txFees"],
						"blockReward": block["blockReward"],
						"uncleReward": block["uncleReward"],
						"to":          block["miner"],
						"type":        "REWARD",
					})
					return dTraces
				}(),
			},
		})
	}

	performSaveToDB := func() {
		var wg sync.WaitGroup

		saveToDB := func(table string, values interface{}) {
			defer wg.Done()
			if values == nil {
				return
			}

			log.Info("InsertBlock - saveToDB", "table", table)

			var err error
			if table == DbTables["transactions"] && len(values.([]interface{})) > 0 {
				log.Info("InsertBlock - saveToDB", "table", table, "len", len(values.([]interface{})))
				_, err = r.DB(e.dbName).Table(DbTables[table]).Insert(values, r.InsertOpts{
					Conflict:      "replace",
					ReturnChanges: "always",
				}).Field("changes").ForEach(func(change r.Term) interface{} {
					return r.Branch(
						change.Field("old_val"), change.Field("old_val").Field("pending").Branch(
							r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(
								func(post r.Term) interface{} {
									return map[string]interface{}{"pendingTxs": post.Field("pendingTxs").Sub(1).Default(0)}
								}), r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(map[string]interface{}{})),
						r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(map[string]interface{}{}))
				}).RunWrite(e.session)
			} else {
				_, err = r.DB(e.dbName).Table(DbTables[table]).Insert(values, r.InsertOpts{
					Conflict: "replace",
				}).RunWrite(e.session)
			}

			if err != nil {
				panic(err)
			}
		}

		updateNonceHashes := func() {
			defer wg.Done()
			for _, tx := range tTxs {
				tx, ok := tx.(map[string]interface{})
				if !ok {
					panic(ok)
				}

				_, err := r.Expr(map[string]interface{}{"changes": make([]interface{}, 0)}).Merge(r.DB(e.dbName).Table(DbTables["transactions"]).GetAllByIndex("nonceHash", tx["nonceHash"]).Update(map[string]interface{}{"replacedBy": tx["hash"], "pending": false}, r.UpdateOpts{
					ReturnChanges: true,
				})).Field("changes").ForEach(func(change r.Term) interface{} {
					return r.Branch(
						change.Field("old_val"), change.Field("old_val").Field("pending").Branch(
							r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(
								func(post r.Term) interface{} {
									return map[string]interface{}{"pendingTxs": post.Field("pendingTxs").Sub(1).Default(0)}
								}), r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(map[string]interface{}{})),
						r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(map[string]interface{}{}))
				}).RunWrite(e.session)

				if err != nil {
					panic(err)
				}
			}
		}

		// Write first nonce hashes
		wg.Add(1)
		go updateNonceHashes()
		wg.Wait()

		// Write after transactions, logs and traces
		wg.Add(3)
		go saveToDB(DbTables["transactions"], tTxs)
		go saveToDB(DbTables["logs"], tLogs)
		go saveToDB(DbTables["traces"], tTrace)
		wg.Wait()

		// After that write blocks and blocks_metrics
		wg.Add(2)
		go saveToDB(DbTables["blocks"], block)
		go saveToDB(DbTables["block_stats"], blockStats)
		wg.Wait()
	}

	go performSaveToDB()
}

// InsertPendingTx Validates and store pending tx into DB
func (e *EthVM) InsertPendingTx(stateDb *state.StateDB, tx *PendingTx) {
	if !e.isEnabled() {
		return
	}

	pTxs := []*PendingTx{tx}
	e.InsertPendingTxs(stateDb, pTxs)
}

// InsertPendingTxs Validates and store pending txs into DB
func (e *EthVM) InsertPendingTxs(stateDb *state.StateDB, txs []*PendingTx) {
	if !e.isEnabled() {
		return
	}

	processTxs := func(stateDb *state.StateDB, pTxs []*PendingTx) chan []interface{} {
		var (
			c      = make(chan []interface{})
			ts     = big.NewInt(time.Now().Unix())
			ptxs   []interface{}
			logs   []interface{}
			traces []interface{}
		)

		go func() {
			for _, pTx := range pTxs {
				var tReceipts types.Receipts
				txBlock := BlockTx{
					Tx:        pTx.Tx,
					Trace:     pTx.Trace,
					Pending:   true,
					Timestamp: ts,
				}
				var tBlockIn = &BlockIn{
					Receipts: append(tReceipts, pTx.Receipt),
					Block:    pTx.Block,
					State:    pTx.State,
					Signer:   pTx.Signer,
				}
				ttx, tlogs, ttrace := formatTx(tBlockIn, txBlock, 0)
				if ttx != nil {
					ptxs = append(ptxs, ttx)
				}
				if tlogs != nil {
					logs = append(logs, tlogs)
				}
				if ttrace != nil {
					traces = append(traces, ttrace)
				}
			}
			var results []interface{}
			results = append(results, ptxs)
			results = append(results, logs)
			results = append(results, traces)
			c <- results
		}()

		return c
	}

	saveToDB := func(table string, values interface{}) {
		if values == nil {
			return
		}

		log.Info("InsertPendingTxs - saveToDB", "table", table)

		result, err := r.DB(e.dbName).Table(DbTables[table]).Insert(values, r.InsertOpts{Conflict: func(id r.Term, oldDoc r.Term, newDoc r.Term) interface{} {
			return newDoc
		}}).RunWrite(e.session)
		if err != nil {
			panic(err)
		}

		if table == DbTables["transactions"] && result.Inserted > 0 {
			r.DB(e.dbName).Table(DbTables["data"]).Get("cached").Update(map[string]interface{}{"pendingTxs": r.Row.Field("pendingTxs").Add(result.Inserted).Default(0)}).RunWrite(e.session)
		}
	}

	// Start processing txs
	pTxsCh := processTxs(stateDb, txs)
	go func(pTxsCh chan []interface{}) {
		select {
		case v, _ := <-pTxsCh:
			// Save processed results to DB
			go saveToDB(DbTables["transactions"], v[0])
			go saveToDB(DbTables["logs"], v[1])
			go saveToDB(DbTables["traces"], v[2])
			return
		}
	}(pTxsCh)
}

// RemovePendingTx Removes a pending transaction from the DB
func (e *EthVM) RemovePendingTx(hash common.Hash) {
	if !e.isEnabled() {
		return
	}

	log.Debug("RemovePendingTx - Removing pending tx", "hash", hash)

	deleteFromDb := func(table string, hash common.Hash) {
		f := map[string]interface{}{"hash": hash.Bytes()}
		_, err := r.DB(e.dbName).Table(DbTables[table]).Filter(f).Delete().Run(e.session)
		if err != nil {
			panic(err)
		}
	}

	go deleteFromDb(DbTables["transactions"], hash)
}

// ----------------
// Helper functions
// ----------------

func formatTx(blockIn *BlockIn, txBlock BlockTx, index int) (interface{}, map[string]interface{}, map[string]interface{}) {
	tx := txBlock.Tx
	receipt := blockIn.Receipts[index]
	if receipt == nil {
		log.Debug("formatTx - Receipt not found for transaction", "hash", tx.Hash())
		return nil, nil, nil
	}

	head := blockIn.Block.Header()
	signer := blockIn.Signer
	from, _ := types.Sender(signer, tx)
	_v, _r, _s := tx.RawSignatureValues()
	fromBalance := blockIn.State.GetBalance(from)
	toBalance := big.NewInt(0)
	if tx.To() != nil {
		toBalance = blockIn.State.GetBalance(*tx.To())
	}

	formatTopics := func(topics []common.Hash) [][]byte {
		arrTopics := make([][]byte, len(topics))
		for i, topic := range topics {
			arrTopics[i] = topic.Bytes()
		}
		return arrTopics
	}

	formatLogs := func(logs []*types.Log) interface{} {
		dLogs := make([]interface{}, len(logs))
		for i, log := range logs {
			logFields := map[string]interface{}{
				"address":     log.Address.Bytes(),
				"topics":      formatTopics(log.Topics),
				"data":        log.Data,
				"blockNumber": big.NewInt(int64(log.BlockNumber)).Bytes(),
				"txHash":      log.TxHash.Bytes(),
				"txIndex":     big.NewInt(int64(log.TxIndex)).Bytes(),
				"blockHash":   log.BlockHash.Bytes(),
				"index":       big.NewInt(int64(log.Index)).Bytes(),
				"removed":     log.Removed,
			}
			dLogs[i] = logFields
		}
		return dLogs
	}

	rfields := map[string]interface{}{
		"cofrom":           nil,
		"root":             blockIn.Block.Header().ReceiptHash.Bytes(),
		"blockHash":        blockIn.Block.Hash().Bytes(),
		"blockNumber":      head.Number.Bytes(),
		"blockIntNumber":   hexutil.Uint64(head.Number.Uint64()),
		"transactionIndex": big.NewInt(int64(index)).Bytes(),
		"from":             from.Bytes(),
		"fromBalance":      fromBalance.Bytes(),
		"to": func() []byte {
			if tx.To() == nil {
				return common.BytesToAddress(make([]byte, 1)).Bytes()
			}
			return tx.To().Bytes()
		}(),
		"toBalance":         toBalance.Bytes(),
		"gasUsed":           big.NewInt(int64(receipt.GasUsed)).Bytes(),
		"cumulativeGasUsed": big.NewInt(int64(receipt.CumulativeGasUsed)).Bytes(),
		"contractAddress":   nil,
		"logsBloom":         receipt.Bloom.Bytes(),
		"gas":               big.NewInt(int64(tx.Gas())).Bytes(),
		"gasPrice":          tx.GasPrice().Bytes(),
		"hash":              tx.Hash().Bytes(),
		"nonceHash":         crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(tx.Nonce())).Bytes()).Bytes(),
		"replacedBy":        make([]byte, 0),
		"input":             tx.Data(),
		"nonce":             big.NewInt(int64(tx.Nonce())).Bytes(),
		"value":             tx.Value().Bytes(),
		"v":                 (_v).Bytes(),
		"r":                 (_r).Bytes(),
		"s":                 (_s).Bytes(),
		"status":            receipt.Status,
		"pending":           txBlock.Pending,
		"data":              tx.Data(),
		"timestamp":         txBlock.Timestamp.Bytes(),
	}

	rlogs := map[string]interface{}{
		"hash":           tx.Hash().Bytes(),
		"blockHash":      blockIn.Block.Hash().Bytes(),
		"blockNumber":    head.Number.Bytes(),
		"blockIntNumber": hexutil.Uint64(head.Number.Uint64()),
		"logs":           formatLogs(receipt.Logs),
	}

	getTxTransfer := func() []map[string]interface{} {
		var dTraces []map[string]interface{}
		dTraces = append(dTraces, map[string]interface{}{
			"op":    "TX",
			"from":  rfields["from"],
			"to":    rfields["to"],
			"value": rfields["value"],
			"input": rfields["input"],
		})
		return dTraces
	}

	rTrace := map[string]interface{}{
		"hash":           tx.Hash().Bytes(),
		"blockHash":      blockIn.Block.Hash().Bytes(),
		"blockNumber":    head.Number.Bytes(),
		"blockIntNumber": hexutil.Uint64(head.Number.Uint64()),
		"trace": func() interface{} {
			temp, ok := txBlock.Trace.(map[string]interface{})
			if !ok {
				temp = map[string]interface{}{
					"isError": true,
					"msg":     txBlock.Trace,
				}
			}
			isError := temp["isError"].(bool)
			transfers, ok := temp["transfers"].([]map[string]interface{})
			if !isError && !ok {
				temp["transfers"] = getTxTransfer()
			} else {
				temp["transfers"] = append(transfers, getTxTransfer()[0])
			}
			return temp
		}(),
	}

	if len(receipt.Logs) == 0 {
		rlogs["logs"] = nil
		rfields["logsBloom"] = nil
	}

	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		rfields["contractAddress"] = receipt.ContractAddress
	}

	arr := make([]interface{}, 2)
	if tx.To() == nil {
		arr[0] = rfields["contractAddress"]
	} else {
		arr[0] = rfields["to"]
	}
	arr[1] = rfields["from"]
	rfields["cofrom"] = arr

	return rfields, rlogs, rTrace
}

func formatBlockStats(blockIn *BlockIn, block *types.Block, bm BlockStats) (map[string]interface{}, error) {
	head := block.Header() // copies the header once
	minerBalance := blockIn.State.GetBalance(head.Coinbase)
	txfees, blockReward, uncleReward := func() (*big.Int, *big.Int, *big.Int) {
		var (
			txfees  *big.Int
			uncleRW *big.Int
			blockRW *big.Int
		)
		if blockIn.TxFees != nil {
			txfees = blockIn.TxFees
		} else {
			txfees = big.NewInt(0)
		}
		if blockIn.IsUncle {
			blockRW = blockIn.UncleReward
			uncleRW = big.NewInt(0)
		} else {
			blockR, uncleR := blockIn.BlockRewardFunc(block)
			blockRW, uncleRW = blockR, uncleR
		}
		return txfees, blockRW, uncleRW
	}()

	bfields := map[string]interface{}{
		"hash":      head.Hash().Bytes(),
		"number":    head.Number,
		"intNumber": hexutil.Uint64(head.Number.Uint64()),
		"timestamp": time.Unix(head.Time.Int64(), 0),

		"size":       int64(hexutil.Uint64(block.Size())),
		"difficulty": head.Difficulty,

		"gasLimit":    int64(head.GasLimit),
		"gasUsed":     int64(head.GasUsed),
		"avgGasPrice": bm.avgGasPrice.Uint64(),

		"txFees":    txfees.Uint64(),
		"avgTxFees": bm.avgTxFees.Uint64(),

		"totalTxs":      bm.totalTxs,
		"successfulTxs": bm.successfulTxs,
		"failedTxs":     bm.failedTxs,
		"pendingTxs":    bm.pendingTxs,

		"accounts":    bm.accounts,
		"newAccounts": bm.newAccounts,

		"isUncle": blockIn.IsUncle,

		"blockReward": blockReward.Uint64(),
		"uncleReward": uncleReward.Uint64(),

		"miner":        head.Coinbase.Bytes(),
		"minerBalance": minerBalance,
	}

	return bfields, nil
}
