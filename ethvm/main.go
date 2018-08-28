// Copyright 2018 The enKryptIO Authors
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
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/segmentio/kafka-go"
	"gopkg.in/urfave/cli.v1"
	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

var (
	// EthVMFlag Enables ETHVM to listen on Ethereum data
	EthVMFlag = cli.BoolFlag{
		Name:  "ethvm",
		Usage: "Enables EthVM to listen every data produced on this node",
	}

	// EthVMBrokersFlag Specifies a list of kafka brokers to connect
	EthVMBrokersFlag = cli.StringFlag{
		Name:  "ethvm-brokers",
		Usage: "Specifies a list of kafka brokers to connect",
	}

	// EthVMBlocksTopicFlag Name of the kafka block topic
	EthVMBlocksTopicFlag = cli.StringFlag{
		Name:  "ethvm-blocks-topic",
		Usage: "Name of the kafka block topic",
	}

	// EthVMPendingTxsTopicFlag Name of the kafka pending txs topic
	EthVMPendingTxsTopicFlag = cli.StringFlag{
		Name:  "ethvm-pending-txs-topic",
		Usage: "Name of the kafka pending txs topic",
	}

	// TraceStr Javascript definition for the tracer that analyzes transactions
	TraceStr = "{transfers:[],isError:false,msg:'',result:function(){var _this=this;return{transfers:_this.transfers,isError:_this.isError,msg:_this.msg}},step:function(log,db){var _this=this;if(log.err){_this.isError=true;_this.msg=log.err.Error();return}var op=log.op;var stack=log.stack;var memory=log.memory;var transfer={};var from=log.account;if(op.toString()=='CALL'){transfer={op:'CALL',value:stack.peek(2).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.BigToAddress(stack.peek(1)),toBalance:db.getBalance(big.BigToAddress(stack.peek(1))).Bytes(),input:memory.slice(big.ToInt(stack.peek(3)),big.ToInt(stack.peek(3))+big.ToInt(stack.peek(4)))};_this.transfers.push(transfer)}else if(op.toString()=='SELFDESTRUCT'){transfer={op:'SELFDESTRUCT',value:db.getBalance(from).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.BigToAddress(stack.peek(0)),toBalance:db.getBalance(big.BigToAddress(stack.peek(0))).Bytes()};_this.transfers.push(transfer)}else if(op.toString()=='CREATE'){transfer={op:'CREATE',value:stack.peek(0).Bytes(),from:from,fromBalance:db.getBalance(from).Bytes(),to:big.CreateContractAddress(from,db.getNonce(from)),toBalance:db.getBalance(big.CreateContractAddress(from,db.getNonce(from))).Bytes()input:memory.slice(big.ToInt(stack.peek(1)),big.ToInt(stack.peek(1))+big.ToInt(stack.peek(2)))};_this.transfers.push(transfer)}}}"

	// Block rewards
	big0  = big.NewInt(0)
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

type BlockIn struct {
	Block           *types.Block
	BlockTxs        *[]BlockTx
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
func NewBlockIn(block *types.Block, txBlocks *[]BlockTx, td *big.Int, receipts []*types.Receipt, signer types.Signer, txFees *big.Int, blockReward *big.Int) *BlockIn {
	return &BlockIn{
		Block:    block,
		BlockTxs: txBlocks,
		PrevTd:   td,
		Receipts: receipts,
		Signer:   signer,
		IsUncle:  false,
		TxFees:   txFees,
		BlockRewardFunc: func(block *types.Block) (*big.Int, *big.Int) {
			if blockReward.Cmp(big0) == 0 {
				return blockReward, blockReward
			}

			reward := new(big.Int).Set(blockReward)
			multiplier := new(big.Int).Div(blockReward, big32)
			uncleReward := new(big.Int).Mul(multiplier, big.NewInt(int64(len(block.Uncles()))))
			return reward, uncleReward
		},
		UncleRewardFunc: func(uncles []*types.Header, index int) *big.Int {
			if blockReward.Cmp(big0) == 0 {
				return blockReward
			}

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

type BlockTx struct {
	Tx        *types.Transaction
	Trace     interface{}
	Pending   bool
	Timestamp *big.Int
}

type PendingTx struct {
	Block   *types.Block
	Tx      *types.Transaction
	Trace   interface{}
	State   *state.StateDB
	Signer  types.Signer
	Receipt *types.Receipt
}

type kblock struct {
	Number            []byte        `json:"number"            gencodec:"required"`
	Hash              []byte        `json:"hash"              gencodec:"required"`
	ParentHash        []byte        `json:"parentHash"        gencodec:"required"`
	IsUncle           bool          `json:"isUncle"           gencodec:"required"`
	Timestamp         []byte        `json:"timestamp"         gencodec:"required"`
	Nonce             [8]byte       `json:"nonce"             gencodec:"required"`
	MixHash           []byte        `json:"mixHash"           gencodec:"required"`
	Sha3Uncles        []byte        `json:"sha3Uncles"        gencodec:"required"`
	LogsBloom         []byte        `json:"logsBloom"         gencodec:"required"`
	StateRoot         []byte        `json:"stateRoot"         gencodec:"required"`
	Miner             []byte        `json:"miner"             gencodec:"required"`
	Difficulty        []byte        `json:"difficulty"        gencodec:"required"`
	TotalDifficulty   []byte        `json:"totalDifficulty"   gencodec:"required"`
	ExtraData         []byte        `json:"extraData"         gencodec:"required"`
	Size              []byte        `json:"size"              gencodec:"required"`
	GasLimit          []byte        `json:"gasLimit"          gencodec:"required"`
	GasUsed           []byte        `json:"gasUsed"           gencodec:"required"`
	TransactionsRoot  []byte        `json:"transactionsRoot"  gencodec:"required"`
	TransactionHashes []byte        `json:"transactionHashes" gencodec:"required"`
	Transactions      []interface{} `json:"transactions"      gencodec:"required"`
	ReceiptsRoot      []byte        `json:"receiptsRoot"      gencodec:"required"`
	UncleHashes       [][]byte      `json:"uncleHashes"       gencodec:"required"`
	Logs              []interface{} `json:"logs"              gencodec:"required"`
	Traces            []interface{} `json:"traces"            gencodec:"required"`
	TxsFees           []byte        `json:"txsFees"           gencodec:"required"`
	BlockReward       []byte        `json:"blockReward"       gencodec:"required"`
	UncleReward       []byte        `json:"uncleReward"       gencodec:"required"`
}

//type ktransaction struct {
//}

type klog struct {
	Address []byte   `json:"address"  gencodec:"required"`
	Topics  [][]byte `json:"topics"   gencodec:"required"`
	Data    []byte   `json:"data"     gencodec:"required"`
	TxHash  []byte   `json:"txHash"   gencodec:"required"`
	TxIndex []byte   `json:"txIndex"  gencodec:"required"`
	Index   []byte   `json:"index"    gencodec:"required"`
	Removed bool     `json:"removed"  gencodec:"required"`
}

type ktraces struct{}

// -----------------
// Main EthVM struct
// -----------------

// EthVM Struct that holds metadata related to EthVM
type EthVM struct {
	enabled bool

	// Kafka
	brokers     string
	blocksTopic string
	pTxsTopic   string

	blocksW *kafka.Writer
	pTxsW   *kafka.Writer
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
			b := "localhost:9092"
			if ctx.GlobalString(EthVMBrokersFlag.Name) != "" {
				b = ctx.GlobalString(EthVMBrokersFlag.Name)
			}
			return b
		}(),
		blocksTopic: func() string {
			topic := "raw-blocks"
			if ctx.GlobalString(EthVMBlocksTopicFlag.Name) != "" {
				topic = ctx.GlobalString(EthVMBlocksTopicFlag.Name)
			}
			return topic
		}(),
		pTxsTopic: func() string {
			topic := "raw-pending-txs"
			if ctx.GlobalString(EthVMPendingTxsTopicFlag.Name) != "" {
				topic = ctx.GlobalString(EthVMPendingTxsTopicFlag.Name)
			}
			return topic
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
		Topic:   e.blocksTopic,
	})

	e.pTxsW = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{e.brokers},
		Topic:   e.pTxsTopic,
	})
}

// InsertBlock Adds a new Block to EthVM
func (e *EthVM) InsertBlock(state *state.StateDB, blockIn *BlockIn) {
	if !e.isEnabled() {
		return
	}

	// Send to Kafka
	go func() {
		b, er := marshalJSON(state, blockIn)
		if er != nil {
			panic(er)
		}

		err := e.blocksW.WriteMessages(context.Background(), kafka.Message{
			Key:   blockIn.Block.Hash().Bytes(),
			Value: b,
		})
		if err != nil {
			panic(err)
		}
	}()
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

	processTxs := func(state *state.StateDB, pendingTxs []*PendingTx) chan []interface{} {
		var (
			c      = make(chan []interface{})
			ts     = big.NewInt(time.Now().Unix())
			pTxs   []interface{}
			logs   []interface{}
			traces []interface{}
		)

		go func() {
			for _, pTx := range pendingTxs {
				var tReceipts types.Receipts
				blockTx := BlockTx{
					Tx:        pTx.Tx,
					Trace:     pTx.Trace,
					Pending:   true,
					Timestamp: ts,
				}
				var tBlockIn = &BlockIn{
					Receipts: append(tReceipts, pTx.Receipt),
					Block:    pTx.Block,
					Signer:   pTx.Signer,
				}
				ttx, tLogs, tTrace := formatTx(state, tBlockIn, blockTx, 0)
				if ttx != nil {
					pTxs = append(pTxs, ttx)
				}
				if tLogs != nil {
					logs = append(logs, tLogs)
				}
				if tTrace != nil {
					traces = append(traces, tTrace)
				}
			}
			var results []interface{}
			results = append(results, pTxs)
			results = append(results, logs)
			results = append(results, traces)
			c <- results
		}()

		return c
	}

	// Send to Kafka
	go func() {
		pTxsChan := processTxs(stateDb, txs)
		results := <-pTxsChan

		pTxs := results[0].([]interface{})
		for i, pTx := range pTxs {
			key := txs[i]

			p, er := json.Marshal(pTx)
			if er != nil {
				panic(e)
			}

			err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
				Key:   key.Tx.Hash().Bytes(),
				Value: p,
			})
			if err != nil {
				panic(err)
			}
		}
	}()
}

// RemovePendingTx Removes a pending transaction from the DB
func (e *EthVM) RemovePendingTx(hash common.Hash) {
	if !e.isEnabled() {
		return
	}

	log.Debug("RemovePendingTx - Removing pending tx", "hash", hash)

	// Send to Kafka
	go func() {
		e.pTxsW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Remove pending tx!"),
		})
	}()
}

// ----------------
// Helper functions
// ----------------

func marshalJSON(state *state.StateDB, in *BlockIn) ([]byte, error) {
	processTxs := func(blockTxs *[]BlockTx) ([][]byte, []interface{}, []interface{}, []interface{}) {
		var (
			tHashes [][]byte
			tTxs    []interface{}
			tLogs   []interface{}
			tTrace  []interface{}
		)
		if blockTxs == nil {
			return tHashes, tTxs, tLogs, tTrace
		}

		for i, blockTx := range *blockTxs {
			_tTx, _tLogs, _tTrace := formatTx(state, in, blockTx, i)
			tTxs = append(tTxs, _tTx)

			if _tLogs["logs"] != nil {
				tLogs = append(tLogs, _tLogs)
			}

			if _tTrace["trace"] != nil {
				tTrace = append(tTrace, _tTrace)
			}

			tHashes = append(tHashes, blockTx.Tx.Hash().Bytes())
		}

		return tHashes, tTxs, tLogs, tTrace
	}
	calculateReward := func(in *BlockIn) ([]byte, []byte, []byte) {
		var (
			txFees      []byte
			blockReward []byte
			uncleReward []byte
		)
		if in.TxFees != nil {
			txFees = in.TxFees.Bytes()
		} else {
			txFees = make([]byte, 0)
		}
		if in.IsUncle {
			blockReward = in.UncleReward.Bytes()
			uncleReward = make([]byte, 0)
		} else {
			blockR, uncleR := in.BlockRewardFunc(in.Block)
			blockReward, uncleReward = blockR.Bytes(), uncleR.Bytes()
		}
		return txFees, blockReward, uncleReward
	}

	block := in.Block
	header := block.Header()
	tHashes, tTxs, tLogs, tTrace := processTxs(in.BlockTxs)
	txFees, blockReward, uncleReward := calculateReward(in)
	td := func() []byte {
		if in.PrevTd == nil {
			return make([]byte, 0)
		}
		return (new(big.Int).Add(block.Difficulty(), in.PrevTd)).Bytes()
	}()

	blockie := &kblock{
		Number:           header.Number.Bytes(),
		Hash:             header.Hash().Bytes(),
		ParentHash:       header.Hash().Bytes(),
		IsUncle:          in.IsUncle,
		Timestamp:        header.Time.Bytes(),
		Nonce:            header.Nonce,
		MixHash:          header.MixDigest.Bytes(),
		Sha3Uncles:       header.UncleHash.Bytes(),
		LogsBloom:        header.Bloom.Bytes(),
		StateRoot:        header.Root.Bytes(),
		Miner:            header.Coinbase.Bytes(),
		Difficulty:       header.Difficulty.Bytes(),
		TotalDifficulty:  td,
		ExtraData:        header.Extra,
		Size:             big.NewInt(int64(hexutil.Uint64(block.Size()))).Bytes(),
		GasLimit:         big.NewInt(int64(header.GasLimit)).Bytes(),
		GasUsed:          big.NewInt(int64(header.GasUsed)).Bytes(),
		TransactionsRoot: header.TxHash.Bytes(),
		Transactions:     tTxs,
		ReceiptsRoot:     header.ReceiptHash.Bytes(),
		UncleHashes:      tHashes,
		Logs:             tLogs,
		Traces:           tTrace,
		TxsFees:          txFees,
		BlockReward:      blockReward,
		UncleReward:      uncleReward,

		// TODO: Finish implementation
		//UncleHashes: func() [][]byte {
		//	uncles := make([][]byte, len(block.Uncles()))
		//	for i, uncle := range block.Uncles() {
		//		uncles[i] = uncle.Hash().Bytes()
		//		e.InsertBlock(state, &BlockIn{
		//			Block:       types.NewBlockWithHeader(uncle),
		//			IsUncle:     true,
		//			UncleReward: blockIn.UncleRewardFunc(block.Uncles(), i),
		//		})
		//	}
		//	return uncles
		//}()
	}

	return json.Marshal(blockie)
}

func formatTx(state *state.StateDB, blockIn *BlockIn, txBlock BlockTx, index int) (interface{}, map[string]interface{}, map[string]interface{}) {
	tx := txBlock.Tx
	receipt := blockIn.Receipts[index]
	if receipt == nil {
		return nil, nil, nil
	}

	head := blockIn.Block.Header()
	signer := blockIn.Signer
	from, _ := types.Sender(signer, tx)
	_v, _r, _s := tx.RawSignatureValues()
	fromBalance := state.GetBalance(from)
	toBalance := big.NewInt(0)
	if tx.To() != nil {
		toBalance = state.GetBalance(*tx.To())
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
		for i, l := range logs {
			logFields := map[string]interface{}{
				"address":     l.Address.Bytes(),
				"topics":      formatTopics(l.Topics),
				"data":        l.Data,
				"blockNumber": big.NewInt(int64(l.BlockNumber)).Bytes(),
				"txHash":      l.TxHash.Bytes(),
				"txIndex":     big.NewInt(int64(l.TxIndex)).Bytes(),
				"blockHash":   l.BlockHash.Bytes(),
				"index":       big.NewInt(int64(l.Index)).Bytes(),
				"removed":     l.Removed,
			}
			dLogs[i] = logFields
		}
		return dLogs
	}

	rFields := map[string]interface{}{
		"root":             blockIn.Block.Header().ReceiptHash.Bytes(),
		"blockHash":        blockIn.Block.Hash().Bytes(),
		"blockNumber":      head.Number.Bytes(),
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

	rLogs := map[string]interface{}{
		"hash":        tx.Hash().Bytes(),
		"blockHash":   blockIn.Block.Hash().Bytes(),
		"blockNumber": head.Number.Bytes(),
		"logs":        formatLogs(receipt.Logs),
	}

	getTxTransfer := func() []map[string]interface{} {
		var dTraces []map[string]interface{}
		dTraces = append(dTraces, map[string]interface{}{
			"op":    "TX",
			"from":  rFields["from"],
			"to":    rFields["to"],
			"value": rFields["value"],
			"input": rFields["input"],
		})
		return dTraces
	}

	rTrace := map[string]interface{}{
		"hash":        tx.Hash().Bytes(),
		"blockHash":   blockIn.Block.Hash().Bytes(),
		"blockNumber": head.Number.Bytes(),
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
		rLogs["logs"] = nil
		rFields["logsBloom"] = nil
	}

	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (common.Address{}) {
		rFields["contractAddress"] = receipt.ContractAddress
	}

	arr := make([]interface{}, 2)
	if tx.To() == nil {
		arr[0] = rFields["contractAddress"]
	} else {
		arr[0] = rFields["to"]
	}
	arr[1] = rFields["from"]

	return rFields, rLogs, rTrace
}
