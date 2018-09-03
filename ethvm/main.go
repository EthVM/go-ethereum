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

	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/segmentio/kafka-go"
	"gopkg.in/urfave/cli.v1"
	"math"
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
	Signer          types.Signer
	IsUncle         bool
	TxFees          *big.Int
	BlockRewardFunc func(block *types.Block) (*big.Int, *big.Int)
	UncleRewardFunc func(uncles []*types.Header, index int) *big.Int
	UncleReward     *big.Int
	Status          byte
}

func (in *BlockIn) marshalJSON(state *state.StateDB) ([]byte, error) {
	processTxs := func(blockTxs *[]BlockTx) []kTransaction {
		if blockTxs == nil {
			return make([]kTransaction, 0)
		}

		kTxs := make([]kTransaction, len(*blockTxs))
		for i, blockTx := range *blockTxs {
			header := in.Block.Header()
			tx := blockTx.Tx
			receipt := blockTx.Receipt
			if receipt == nil {
				continue
			}
			signer := in.Signer
			from, _ := types.Sender(signer, tx)
			_v, _r, _s := tx.RawSignatureValues()
			fromBalance := state.GetBalance(from)
			to := func() []byte {
				if tx.To() == nil {
					return common.BytesToAddress(make([]byte, 1)).Bytes()
				}
				return tx.To().Bytes()
			}()
			toBalance := big.NewInt(0)
			if tx.To() != nil {
				toBalance = state.GetBalance(*tx.To())
			}
			value := tx.Value().Bytes()
			input := tx.Data()

			kTx := kTransaction{
				Hash:              tx.Hash().Bytes(),
				Root:              header.ReceiptHash.Bytes(),
				Index:             big.NewInt(int64(i)).Bytes(),
				Timestamp:         blockTx.Timestamp.Bytes(),
				Nonce:             big.NewInt(int64(tx.Nonce())).Bytes(),
				NonceHash:         crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(tx.Nonce())).Bytes()).Bytes(),
				From:              from.Bytes(),
				FromBalance:       fromBalance.Bytes(),
				To:                to,
				ToBalance:         toBalance.Bytes(),
				Input:             input,
				Gas:               big.NewInt(int64(tx.Gas())).Bytes(),
				GasPrice:          tx.GasPrice().Bytes(),
				GasUsed:           big.NewInt(int64(receipt.GasUsed)).Bytes(),
				CumulativeGasUsed: big.NewInt(int64(receipt.CumulativeGasUsed)).Bytes(),
				ContractAddress: func() []byte {
					if receipt.ContractAddress != (common.Address{}) {
						return receipt.ContractAddress.Bytes()
					}
					return make([]byte, 0)
				}(),
				LogsBloom: receipt.Bloom.Bytes(),
				Value:     value,
				R:         (_r).Bytes(),
				V:         (_v).Bytes(),
				S:         (_s).Bytes(),
				Status:    big.NewInt(int64(receipt.Status)).Bytes(),
				Logs: func() []kLog {
					var logs []kLog

					rawLogs := receipt.Logs
					if rawLogs == nil || len(rawLogs) == 0 {
						return make([]kLog, 0)
					}

					for _, rawLog := range rawLogs {
						if rawLog == nil {
							continue
						}
						l := kLog{
							Address: rawLog.Address.Bytes(),
							Topics: func(rawTopics []common.Hash) [][]byte {
								topics := make([][]byte, len(rawTopics))
								for i, rawTopic := range rawTopics {
									topics[i] = rawTopic.Bytes()
								}
								return topics
							}(rawLog.Topics),
							Data:    rawLog.Data,
							Index:   big.NewInt(int64(rawLog.Index)).Bytes(),
							Removed: rawLog.Removed,
						}
						logs = append(logs, l)
					}

					return logs
				}(),
				Trace: func() []byte {
					getTxTransfer := func() []map[string]interface{} {
						var dTraces []map[string]interface{}
						dTraces = append(dTraces, map[string]interface{}{
							"op":    "TX",
							"from":  from.Bytes(),
							"to":    to,
							"value": value,
							"input": input,
						})
						return dTraces
					}

					raw, ok := blockTx.Trace.(map[string]interface{})
					if !ok {
						raw = map[string]interface{}{
							"isError": true,
							"msg":     blockTx.Trace,
						}
					}
					isError := raw["isError"].(bool)
					transfers, ok := raw["transfers"].([]map[string]interface{})
					if !isError && !ok {
						raw["transfers"] = getTxTransfer()
					} else {
						raw["transfers"] = append(transfers, getTxTransfer()[0])
					}
					res, err := json.Marshal(raw)
					if err != nil {
						panic(err)
					}
					return res
				}(),
			}

			kTxs = append(kTxs, kTx)
		}

		return kTxs
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
	txs := processTxs(in.BlockTxs)
	txFees, blockReward, uncleReward := calculateReward(in)
	td := func() []byte {
		if in.PrevTd == nil {
			return make([]byte, 0)
		}
		return (new(big.Int).Add(block.Difficulty(), in.PrevTd)).Bytes()
	}()

	kBlock := &kBlock{
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
		TransactionsRoot: header.TxHash.Bytes(),
		ReceiptsRoot:     header.ReceiptHash.Bytes(),
		Miner:            header.Coinbase.Bytes(),
		Difficulty:       header.Difficulty.Bytes(),
		TotalDifficulty:  td,
		ExtraData:        header.Extra,
		Size:             big.NewInt(int64(hexutil.Uint64(block.Size()))).Bytes(),
		GasLimit:         big.NewInt(int64(header.GasLimit)).Bytes(),
		GasUsed:          big.NewInt(int64(header.GasUsed)).Bytes(),
		TxsFees:          txFees,
		BlockReward:      blockReward,
		UncleReward:      uncleReward,
		Transactions:     txs,
		Uncles: func() [][]byte {
			uncles := make([][]byte, len(block.Uncles()))
			for i, uncle := range block.Uncles() {
				uncles[i] = uncle.Hash().Bytes()
			}
			return uncles
		}(),
	}

	return json.Marshal(kBlock)
}

// NewBlockIn Creates and formats a new BlockIn instance
func NewBlockIn(block *types.Block, txBlocks *[]BlockTx, td *big.Int, signer types.Signer, txFees *big.Int, blockReward *big.Int, status byte) *BlockIn {
	return &BlockIn{
		Block:    block,
		BlockTxs: txBlocks,
		PrevTd:   td,
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
		Status: status,
	}
}

type BlockTx struct {
	Tx        *types.Transaction
	Trace     interface{}
	Receipt   *types.Receipt
	Logs      []*types.Log
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

// ------------------
// EthVM data structs
// ------------------

type kBlock struct {
	Number           []byte         `json:"number"`
	Hash             []byte         `json:"hash"`
	ParentHash       []byte         `json:"parentHash"`
	IsUncle          bool           `json:"isUncle"`
	IsCanonical      bool           `json:"isCanonical"`
	Timestamp        []byte         `json:"timestamp"`
	Nonce            [8]byte        `json:"nonce"`
	MixHash          []byte         `json:"mixHash"`
	LogsBloom        []byte         `json:"logsBloom"`
	StateRoot        []byte         `json:"stateRoot"`
	TransactionsRoot []byte         `json:"transactionsRoot"`
	ReceiptsRoot     []byte         `json:"receiptsRoot"`
	Miner            []byte         `json:"miner"`
	Difficulty       []byte         `json:"difficulty"`
	TotalDifficulty  []byte         `json:"totalDifficulty"`
	ExtraData        []byte         `json:"extraData"`
	Size             []byte         `json:"size"`
	GasLimit         []byte         `json:"gasLimit"`
	GasUsed          []byte         `json:"gasUsed"`
	Transactions     []kTransaction `json:"transactions"`
	Uncles           [][]byte       `json:"uncles"`
	Sha3Uncles       []byte         `json:"sha3Uncles"`
	TxsFees          []byte         `json:"txsFees"`
	BlockReward      []byte         `json:"blockReward"`
	UncleReward      []byte         `json:"uncleReward"`
}

type kTransaction struct {
	Hash              []byte `json:"hash"`
	Root              []byte `json:"root"`
	Index             []byte `json:"index"`
	Timestamp         []byte `json:"timestamp"`
	Nonce             []byte `json:"nonce"`
	NonceHash         []byte `json:"nonceHash"`
	From              []byte `json:"from"`
	FromBalance       []byte `json:"fromBalance"`
	To                []byte `json:"to"`
	ToBalance         []byte `json:"toBalance"`
	Input             []byte `json:"input"`
	ContractAddress   []byte `json:"contractAddress"`
	Gas               []byte `json:"gas"`
	GasPrice          []byte `json:"gasPrice"`
	GasUsed           []byte `json:"gasUsed"`
	CumulativeGasUsed []byte `json:"cumulativeGasUsed"`
	LogsBloom         []byte `json:"logsBloom"`
	Value             []byte `json:"value"`
	V                 []byte `json:"v"`
	R                 []byte `json:"r"`
	S                 []byte `json:"s"`
	Status            []byte `json:"status"`
	Logs              []kLog `json:"logs"`
	Trace             []byte `json:"trace"`
}

type kLog struct {
	Address []byte   `json:"address"`
	Topics  [][]byte `json:"topics"`
	Data    []byte   `json:"data"`
	TxHash  []byte   `json:"txHash"`
	TxIndex []byte   `json:"txIndex"`
	Index   []byte   `json:"index"`
	Removed bool     `json:"removed"`
}

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
		Brokers:       []string{e.brokers},
		Topic:         e.blocksTopic,
		QueueCapacity: math.MaxInt32,
	})

	e.pTxsW = kafka.NewWriter(kafka.WriterConfig{
		Brokers:       []string{e.brokers},
		Topic:         e.pTxsTopic,
		QueueCapacity: math.MaxInt32,
	})
}

// InsertBlock Adds a new Block to EthVM
func (e *EthVM) InsertBlock(state *state.StateDB, blockIn *BlockIn) {
	if !e.isEnabled() {
		return
	}

	// Send to Kafka
	go func() {
		b, er := blockIn.marshalJSON(state)
		if er != nil {
			panic(er)
		}

		err := e.blocksW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(hexutil.Encode(blockIn.Block.Header().Hash().Bytes())),
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

	// processTxs := func(state *state.StateDB, pendingTxs []*PendingTx) chan []interface{} {
	// 	var (
	// 		c      = make(chan []interface{})
	// 		ts     = big.NewInt(time.Now().Unix())
	// 		pTxs   []interface{}
	// 		logs   []interface{}
	// 		traces []interface{}
	// 	)

	// 	go func() {
	// 		for _, pTx := range pendingTxs {
	// 			var tReceipts types.Receipts
	// 			blockTx := BlockTx{
	// 				Tx:        pTx.Tx,
	// 				Trace:     pTx.Trace,
	// 				Pending:   true,
	// 				Timestamp: ts,
	// 			}
	// 			var tBlockIn = &BlockIn{
	// 				Receipts: append(tReceipts, pTx.Receipt),
	// 				Block:    pTx.Block,
	// 				Signer:   pTx.Signer,
	// 			}
	// 			ttx, tLogs, tTrace := formatTx(state, tBlockIn, blockTx, 0)
	// 			if ttx != nil {
	// 				pTxs = append(pTxs, ttx)
	// 			}
	// 			if tLogs != nil {
	// 				logs = append(logs, tLogs)
	// 			}
	// 			if tTrace != nil {
	// 				traces = append(traces, tTrace)
	// 			}
	// 		}
	// 		var results []interface{}
	// 		results = append(results, pTxs)
	// 		results = append(results, logs)
	// 		results = append(results, traces)
	// 		c <- results
	// 	}()

	// 	return c
	// }

	// Send to Kafka
	// go func() {
	// 	pTxsChan := processTxs(stateDb, txs)
	// 	results := <-pTxsChan

	// 	pTxs := results[0].([]interface{})
	// 	for i, pTx := range pTxs {
	// 		key := txs[i]

	// 		p, er := json.Marshal(pTx)
	// 		if er != nil {
	// 			panic(e)
	// 		}

	// 		err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
	//			Key:   []byte(hexutil.Encode(key)),
	// 			Value: p,
	// 		})
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 	}
	// }()
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
			Value: []byte("Remove pending tx!"),
		})
	}()
}
