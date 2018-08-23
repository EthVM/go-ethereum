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
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/segmentio/kafka-go"
	"gopkg.in/urfave/cli.v1"
	"context"
)

var (
	// EthVMFlag Save blockchain data to external db, make sure to set RETHINKDB_URL env variable
	EthVMFlag = cli.BoolFlag{
		Name:  "ethvm",
		Usage: "Enables EthVM to listen every data on ethereum",
	}

	// EthVMDbNameFlag Select which name will be assigned to the RethinkDB db table
	EthVMBrokersFlag = cli.StringFlag{
		Name:  "ethvm.brokers",
		Usage: "Specifies a list of kafka brokers to connect",
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

type TxBlock struct {
	Tx        *types.Transaction
	Trace     interface{}
	Pending   bool
	Timestamp *big.Int
}

type BlockIn struct {
	Block           *types.Block
	TxBlocks        *[]TxBlock
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
func NewBlockIn(block *types.Block, txBlocks *[]TxBlock, state *state.StateDB, td *big.Int, receipts []*types.Receipt, signer types.Signer, txFees *big.Int, blockReward *big.Int) *BlockIn {
	return &BlockIn{
		Block:    block,
		TxBlocks: txBlocks,
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
			r := new(big.Int)
			for i, uncle := range uncles {
				r.Add(uncle.Number, big8)
				r.Sub(r, block.Header().Number)
				r.Mul(r, blockReward)
				r.Div(r, big8)
				if i == index {
					return r
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

func newTxMetric(blockIn *BlockIn, txBlock TxBlock, index int) TXMetric {
	tx := txBlock.Tx
	receipt := blockIn.Receipts[index]

	// if no receipt, then there is no transaction
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

type BlockMetrics struct {
	totalGasUsed       *big.Int
	avgGasUsed         *big.Int
	totalGasPrice      *big.Int
	avgGasPrice        *big.Int
	accounts           []*common.Address
	newAccounts        []*common.Address
	pendingTransaction uint
	totalTransaction   uint
	successfulTxs      uint
	failedTxs          uint
}

func newBlockMetrics(blockIn *BlockIn) BlockMetrics {
	bm := BlockMetrics{
		avgGasPrice:        big.NewInt(0),
		pendingTransaction: 0,
		totalTransaction:   0,
		failedTxs:          0,
	}

	if blockIn.TxBlocks == nil || blockIn.IsUncle {
		return bm
	}

	totalGasPrice := big.NewInt(0)
	totalGasUsed := big.NewInt(0)

	for i, block := range *blockIn.TxBlocks {
		bm.totalTransaction++
		ttx := newTxMetric(blockIn, block, i)
		if ttx.pending {
			bm.pendingTransaction++
		}

		if ttx.status == uint(types.ReceiptStatusFailed) {
			bm.failedTxs++
		}

		if ttx.status == uint(types.ReceiptStatusSuccessful) {
			bm.successfulTxs++
		}

		bm.accounts = append(bm.accounts, ttx.to)
		bm.accounts = append(bm.accounts, ttx.from)

		if ttx.nonce == 0 {
			bm.newAccounts = append(bm.newAccounts, ttx.from)
		}

		totalGasPrice = totalGasPrice.Add(ttx.gasPrice, totalGasPrice)
		totalGasUsed = totalGasUsed.Add(ttx.gasUsed, totalGasUsed)
	}

	if len(*blockIn.TxBlocks) > 0 {
		avgGasPrice := totalGasPrice.Div(totalGasPrice, big.NewInt(int64(len(*blockIn.TxBlocks))))
		bm.avgGasPrice = avgGasPrice
	}

	bm.totalGasPrice = totalGasPrice
	bm.totalGasUsed = totalGasUsed

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

	// Kafka
	brokers string

	blocksW *kafka.Writer
	txsW    *kafka.Writer
	pTxsW   *kafka.Writer
	logsW   *kafka.Writer
	tracesW *kafka.Writer
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
		brokers: func() string {
			var b = "localhost:9092"
			if ctx.GlobalString(EthVMBrokersFlag.Name) != "" {
				b = ctx.GlobalString(EthVMBrokersFlag.Name)
			}
			return b
		}(),
	}
	return instance
}

func (e *EthVM) isEnabled() bool {
	return e.enabled
}

func (e *EthVM) isConnected() bool {
	return e.isEnabled()
}

// Connect Performs connection to the DB (and creates tables and indices if needed)
func (e *EthVM) Connect() {
	if !e.isEnabled() {
		return
	}

	// Create Kafka writers
	e.blocksW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   "blocks",
	})

	e.txsW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   "txs",
	})

	e.pTxsW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   "pTxs",
	})

	e.logsW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   "logs",
	})

	e.tracesW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   "traces",
	})
}

// InsertGenesisTrace Adds a Genesis Block to the DB
func (e *EthVM) InsertGenesisTrace(gAlloc map[common.Address][]byte, block *types.Block) {
	if !e.isEnabled() {
		return
	}

	//// Format
	//rTrace := map[string]interface{}{
	//	"hash":           common.BytesToHash([]byte("GENESIS_TX")).Bytes(),
	//	"blockHash":      block.Hash().Bytes(),
	//	"blockNumber":    block.Header().Number.Bytes(),
	//	"blockIntNumber": hexutil.Uint64(block.Header().Number.Uint64()),
	//	"trace": map[string]interface{}{
	//		"isError": false,
	//		"msg":     "",
	//		"transfers": func() interface{} {
	//			var dTraces []interface{}
	//			for addr, balance := range gAlloc {
	//				dTraces = append(dTraces, map[string]interface{}{
	//					"op":    "BLOCK",
	//					"value": balance,
	//					"to":    addr.Bytes(),
	//					"type":  "GENESIS",
	//				})
	//			}
	//			dTraces = append(dTraces, map[string]interface{}{
	//				"op":          "BLOCK",
	//				"txFees":      big.NewInt(0).Bytes(),
	//				"blockReward": big.NewInt(5e+18).Bytes(),
	//				"uncleReward": big.NewInt(0).Bytes(),
	//				"to":          common.BytesToAddress(make([]byte, 1)).Bytes(),
	//				"type":        "REWARD",
	//			})
	//			return dTraces
	//		}(),
	//	},
	//}
	//
	//// Send to Kafka
	//go func() {
	//}()

	// Send to Kafka
	send := func() {
		e.tracesW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Insert genesis trace!"),
		})
	}

	go send()
}

// InsertBlock Adds a new Block to the DB
func (e *EthVM) InsertBlock(blockIn *BlockIn) {
	if !e.isEnabled() {
		return
	}

	//processTxs := func(txblocks *[]TxBlock) ([][]byte, []interface{}, []interface{}, []interface{}) {
	//	var (
	//		tHashes [][]byte
	//		tTxs    []interface{}
	//		tLogs   []interface{}
	//		tTrace  []interface{}
	//	)
	//	if txblocks == nil {
	//		log.Info("InsertBlock - processTxs / Empty txblocks")
	//		return tHashes, tTxs, tLogs, tTrace
	//	}
	//
	//	for i, _txBlock := range *txblocks {
	//		_tTx, _tLogs, _tTrace := formatTx(blockIn, _txBlock, i)
	//		tTxs = append(tTxs, _tTx)
	//		if _tLogs["logs"] != nil {
	//			tLogs = append(tLogs, _tLogs)
	//		}
	//		if _tTrace["trace"] != nil {
	//			tTrace = append(tTrace, _tTrace)
	//		}
	//		tHashes = append(tHashes, _txBlock.Tx.Hash().Bytes())
	//	}
	//
	//	log.Info("InsertBlock - processTxs", "tHashes", len(tHashes), "tTxs", len(tTxs), "tLogs", len(tLogs), "tTrace", len(tTrace))
	//	return tHashes, tTxs, tLogs, tTrace
	//}
	//
	//formatBlock := func(block *types.Block, tHashes [][]byte) (map[string]interface{}, error) {
	//	head := block.Header() // copies the header once
	//	minerBalance := blockIn.State.GetBalance(head.Coinbase)
	//
	//	txFees, blockReward, uncleReward := func() ([]byte, []byte, []byte) {
	//		var (
	//			_txfees []byte
	//			_uncleR []byte
	//			_blockR []byte
	//		)
	//		if blockIn.TxFees != nil {
	//			_txfees = blockIn.TxFees.Bytes()
	//		} else {
	//			_txfees = make([]byte, 0)
	//		}
	//		if blockIn.IsUncle {
	//			_blockR = blockIn.UncleReward.Bytes()
	//			_uncleR = make([]byte, 0)
	//		} else {
	//			blockR, uncleR := blockIn.BlockRewardFunc(block)
	//			_blockR, _uncleR = blockR.Bytes(), uncleR.Bytes()
	//
	//		}
	//		return _txfees, _blockR, _uncleR
	//	}()
	//
	//	bfields := map[string]interface{}{
	//		"number":       head.Number.Bytes(),
	//		"intNumber":    hexutil.Uint64(head.Number.Uint64()),
	//		"hash":         head.Hash().Bytes(),
	//		"parentHash":   head.ParentHash.Bytes(),
	//		"nonce":        head.Nonce,
	//		"mixHash":      head.MixDigest.Bytes(),
	//		"sha3Uncles":   head.UncleHash.Bytes(),
	//		"logsBloom":    head.Bloom.Bytes(),
	//		"stateRoot":    head.Root.Bytes(),
	//		"miner":        head.Coinbase.Bytes(),
	//		"minerBalance": minerBalance.Bytes(),
	//		"difficulty":   head.Difficulty.Bytes(),
	//		"totalDifficulty": func() []byte {
	//			if blockIn.PrevTd == nil {
	//				return make([]byte, 0)
	//			}
	//			return (new(big.Int).Add(block.Difficulty(), blockIn.PrevTd)).Bytes()
	//		}(),
	//		"extraData":         head.Extra,
	//		"size":              big.NewInt(int64(hexutil.Uint64(block.Size()))).Bytes(),
	//		"gasLimit":          big.NewInt(int64(head.GasLimit)).Bytes(),
	//		"gasUsed":           big.NewInt(int64(head.GasUsed)).Bytes(),
	//		"timestamp":         head.Time.Bytes(),
	//		"transactionsRoot":  head.TxHash.Bytes(),
	//		"receiptsRoot":      head.ReceiptHash.Bytes(),
	//		"transactionHashes": tHashes,
	//		"uncleHashes": func() [][]byte {
	//			uncles := make([][]byte, len(block.Uncles()))
	//			for i, uncle := range block.Uncles() {
	//				uncles[i] = uncle.Hash().Bytes()
	//				e.InsertBlock(&BlockIn{
	//					Block:       types.NewBlockWithHeader(uncle),
	//					State:       blockIn.State,
	//					IsUncle:     true,
	//					UncleReward: blockIn.UncleRewardFunc(block.Uncles(), i),
	//				})
	//			}
	//			return uncles
	//		}(),
	//		"isUncle":     blockIn.IsUncle,
	//		"txFees":      txFees,
	//		"blockReward": blockReward,
	//		"uncleReward": uncleReward,
	//	}
	//	return bfields, nil
	//}
	//
	//tHashes, tTxs, tLogs, tTrace := processTxs(blockIn.TxBlocks)
	//bm := newBlockMetrics(blockIn)
	//block, _ := formatBlock(blockIn.Block, tHashes)
	//blockMetadata, _ := formatBlockMetric(blockIn, blockIn.Block, bm)
	//
	//if block["intNumber"] != 0 {
	//	tTrace = append(tTrace, map[string]interface{}{
	//		"hash":           block["hash"],
	//		"blockHash":      block["hash"],
	//		"blockNumber":    block["number"],
	//		"blockIntNumber": block["intNumber"],
	//		"trace": map[string]interface{}{
	//			"isError": false,
	//			"msg":     "",
	//			"transfers": func() interface{} {
	//				var dTraces []interface{}
	//				dTraces = append(dTraces, map[string]interface{}{
	//					"op":          "BLOCK",
	//					"txFees":      block["txFees"],
	//					"blockReward": block["blockReward"],
	//					"uncleReward": block["uncleReward"],
	//					"to":          block["miner"],
	//					"type":        "REWARD",
	//				})
	//				return dTraces
	//			}(),
	//		},
	//	})
	//}
	//

	// Send to Kafka
	send := func() {
		e.blocksW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("New block!"),
		})
	}

	go send()
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

	//processTxs := func(stateDb *state.StateDB, pTxs []*PendingTx) chan []interface{} {
	//	var (
	//		c      = make(chan []interface{})
	//		ts     = big.NewInt(time.Now().Unix())
	//		ptxs   []interface{}
	//		logs   []interface{}
	//		traces []interface{}
	//	)
	//
	//	go func() {
	//		for _, pTx := range pTxs {
	//			var tReceipts types.Receipts
	//			txBlock := TxBlock{
	//				Tx:        pTx.Tx,
	//				Trace:     pTx.Trace,
	//				Pending:   true,
	//				Timestamp: ts,
	//			}
	//			var tBlockIn = &BlockIn{
	//				Receipts: append(tReceipts, pTx.Receipt),
	//				Block:    pTx.Block,
	//				State:    pTx.State,
	//				Signer:   pTx.Signer,
	//			}
	//			ttx, tlogs, ttrace := formatTx(tBlockIn, txBlock, 0)
	//			if ttx != nil {
	//				ptxs = append(ptxs, ttx)
	//			}
	//			if tlogs != nil {
	//				logs = append(logs, tlogs)
	//			}
	//			if ttrace != nil {
	//				traces = append(traces, ttrace)
	//			}
	//		}
	//		var results []interface{}
	//		results = append(results, ptxs)
	//		results = append(results, logs)
	//		results = append(results, traces)
	//		c <- results
	//	}()
	//
	//	return c
	//}
	//
	//// Send to Kafka
	//go func() {
	//}()

	// Send to Kafka
	send := func() {
		e.pTxsW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("New pending tx!"),
		})
	}

	go send()
}

// RemovePendingTx Removes a pending transaction from the DB
func (e *EthVM) RemovePendingTx(hash common.Hash) {
	if !e.isEnabled() {
		return
	}

	log.Debug("RemovePendingTx - Removing pending tx", "hash", hash)

	// Send to Kafka
	send := func() {
		e.pTxsW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Remove pending tx!"),
		})
	}

	go send()
}

// ----------------
// Helper functions
// ----------------

func formatTx(blockIn *BlockIn, txBlock TxBlock, index int) (interface{}, map[string]interface{}, map[string]interface{}) {
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

func formatBlockMetric(blockIn *BlockIn, block *types.Block, bm BlockMetrics) (map[string]interface{}, error) {
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

		"totalTxs":      bm.totalTransaction,
		"pendingTxs":    bm.pendingTransaction,
		"successfulTxs": bm.successfulTxs,
		"failedTxs":     bm.failedTxs,

		"avgGasPrice": bm.avgGasPrice.Uint64(),

		"accounts":    bm.accounts,
		"newAccounts": bm.newAccounts,

		"isUncle":     blockIn.IsUncle,
		"blockReward": blockReward.Uint64(),
		"uncleReward": uncleReward.Uint64(),

		"miner":        head.Coinbase.Bytes(),
		"minerBalance": minerBalance,

		"difficulty": head.Difficulty,
		"txFees":     txfees.Uint64(),
		"gasLimit":   int64(head.GasLimit),
		"gasUsed":    int64(head.GasUsed),
		"size":       int64(hexutil.Uint64(block.Size())),
	}

	return bfields, nil
}
