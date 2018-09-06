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

	"bytes"
	"context"
	"encoding/binary"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/segmentio/kafka-go"
	"gopkg.in/urfave/cli.v1"
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
	TraceStr = "{transfers:[],isError:!1,msg:'',result:function(){return{transfers:this.transfers,isError:this.isError,msg:this.msg}},step:function(e,t){if(e.err)return this.isError=!0,void(this.msg=e.err.Error());var r=e.op,s=e.stack,o=e.memory,a={},i=e.account;return'CREATE'==r.toString()?(a={op:'CREATE',value:s.peek(0).Bytes(),from:i,fromBalance:t.getBalance(i).Bytes(),to:big.CreateContractAddress(i,t.getNonce(i)),toBalance:t.getBalance(big.CreateContractAddress(i,t.getNonce(i))).Bytes(),input:o.slice(big.ToInt(s.peek(1)),big.ToInt(s.peek(1))+big.ToInt(s.peek(2)))},void this.transfers.push(a)):'CALL'==r.toString()?(a={op:'CALL',value:s.peek(2).Bytes(),from:i,fromBalance:t.getBalance(i).Bytes(),to:big.BigToAddress(s.peek(1)),toBalance:t.getBalance(big.BigToAddress(s.peek(1))).Bytes(),input:o.slice(big.ToInt(s.peek(3)),big.ToInt(s.peek(3))+big.ToInt(s.peek(4)))},void this.transfers.push(a)):'SELFDESTRUCT'==r.toString()?(a={op:'SELFDESTRUCT',value:t.getBalance(i).Bytes(),from:i,fromBalance:t.getBalance(i).Bytes(),to:big.BigToAddress(s.peek(0)),toBalance:t.getBalance(big.BigToAddress(s.peek(0))).Bytes()},void this.transfers.push(a)):void 0}}"

	// Block rewards
	big0  = big.NewInt(0)
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)

	// Meta values
	ctx *cli.Context

	// Global EthVM instance
	instance *EthVM
)

// ----------
// Generators
// ----------

// Make sure you have installed gogen-avro: https://github.com/actgardner/gogen-avro

//go:generate $GOPATH/bin/gogen-avro --package=ethvm . schemas.v1.avsc

// ------------------
// EthVM data structs
// ------------------

type BlockIn struct {
	Block           *types.Block
	BlockTxs        *[]BlockTx
	PrevTd          *big.Int
	Signer          types.Signer
	IsUncle         bool
	IsCanonical     bool
	TxFees          *big.Int
	BlockRewardFunc func(block *types.Block) (*big.Int, *big.Int)
	UncleRewardFunc func(uncles []*types.Header, index int) *big.Int
	UncleReward     *big.Int
	Status          byte
}

func (in *BlockIn) bytes(state *state.StateDB) []byte {
	block := in.Block
	header := block.Header()
	td := func() int64 {
		if in.PrevTd == nil {
			return big0.Int64()
		}
		return (new(big.Int).Add(block.Difficulty(), in.PrevTd)).Int64()
	}()
	txFees, blockReward, uncleReward := calculateBlockReward(in)
	txs := processBlockTxs(state, in)

	b := &Block{
		Number:           header.Number.Int64(),
		Hash:             header.Hash().Hex(),
		ParentHash:       header.Hash().Hex(),
		MixDigest:        header.MixDigest.Hex(),
		IsUncle:          in.IsUncle,
		IsCanonical:      in.IsCanonical,
		Timestamp:        header.Time.Int64(),
		Nonce:            int64(header.Nonce.Uint64()),
		Sha3Uncles:       header.UncleHash.Hex(),
		LogsBloom:        hexutil.Encode(header.Bloom.Bytes()),
		StateRoot:        hexutil.Encode(header.Root.Bytes()),
		TransactionsRoot: hexutil.Encode(header.ReceiptHash.Bytes()),
		Miner:            header.Coinbase.Hex(),
		Difficulty:       header.Difficulty.Int64(),
		TotalDifficulty:  td,
		ExtraData:        header.Extra,
		Size:             int64(hexutil.Uint64(block.Size())),
		GasLimit:         int64(header.GasLimit),
		GasUsed:          int64(header.GasUsed),
		Transactions:     txs,
		TxsFees:          txFees,
		Uncles: func() []string {
			uncles := make([]string, len(block.Uncles()))
			for i, uncle := range block.Uncles() {
				uncles[i] = uncle.Hash().Hex()
			}
			return uncles
		}(),
		BlockReward: blockReward,
		UncleReward: uncleReward,
	}

	// Encode as Kafka requirements
	buffer := &bytes.Buffer{}

	// 1) Magic bytes
	_, err := buffer.Write([]byte{0})
	if err != nil {
		panic(err)
	}

	// 2) Id, in our case 1
	idSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(idSlice, uint32(1))
	_, err = buffer.Write(idSlice)
	if err != nil {
		panic(err)
	}

	// 3) Encode data
	var buf bytes.Buffer
	b.Serialize(&buf)

	// Append to main buffer
	buffer.Write(buf.Bytes())

	return buffer.Bytes()
}

// NewBlockIn Creates and formats a new BlockIn struct
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

type PendingTxIn struct {
	Tx      *types.Transaction
	Trace   interface{}
	Signer  types.Signer
	Receipt *types.Receipt
	Action  Action
}

func pendingTxBytes(ptx PendingTxs) []byte {
	// Encode as Kafka requirements
	buffer := &bytes.Buffer{}

	// 1) Magic bytes
	_, err := buffer.Write([]byte{0})
	if err != nil {
		panic(err)
	}

	// 2) Id, in our case 1
	idSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(idSlice, uint32(1))
	_, err = buffer.Write(idSlice)
	if err != nil {
		panic(err)
	}

	// 3) Encode data
	var buf bytes.Buffer
	ptx.Serialize(&buf)

	// Append to main buffer
	buffer.Write(buf.Bytes())

	return buf.Bytes()
}

func NewPendingTxIn(tx *types.Transaction, trace interface{}, signer types.Signer, receipt *types.Receipt, action Action) *PendingTxIn {
	return &PendingTxIn{
		Tx:      tx,
		Trace:   trace,
		Signer:  signer,
		Receipt: receipt,
		Action:  action,
	}
}

func UpdatePendingTxIn(tx *types.Transaction, action Action) *PendingTxIn {
	return &PendingTxIn{
		Tx:     tx,
		Action: action,
	}
}

type BlockTx struct {
	Tx        *types.Transaction
	Trace     interface{}
	Receipt   *types.Receipt
	Logs      []*types.Log
	Timestamp *big.Int
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
	err := e.blocksW.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(hexutil.Encode(blockIn.Block.Header().Hash().Bytes())),
		Value: blockIn.bytes(state),
	})
	if err != nil {
		panic(err)
	}
}

// InsertPendingTx Validates and store pending tx into DB
func (e *EthVM) InsertPendingTx(stateDb *state.StateDB, tx *PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	pTxs := []*PendingTxIn{tx}
	e.InsertPendingTxs(stateDb, pTxs)
}

// InsertPendingTxs Validates and store pending txs into DB
func (e *EthVM) InsertPendingTxs(state *state.StateDB, pTxs []*PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	// Send to kafka
	err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
		Value: pendingTxBytes(processInsertionPendingTxs(state, pTxs)),
	})
	if err != nil {
		panic(err)
	}
}

// RemovePendingTxs Removes a pending transaction from the DB
func (e *EthVM) RemovePendingTx(pTx *PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	pTxs := []*PendingTxIn{pTx}
	e.RemovePendingTxs(pTxs)
}

// RemovePendingTxs Removes a pending transaction from the DB
func (e *EthVM) RemovePendingTxs(pTxs []*PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	// Send to Kafka
	e.pTxsW.WriteMessages(context.Background(), kafka.Message{
		Value: pendingTxBytes(processDeletionPendingTxs(pTxs)),
	})
}

// --------------------
// Helpers
// --------------------

func calculateBlockReward(in *BlockIn) (int64, int64, int64) {
	var (
		txFees      int64
		blockReward int64
		uncleReward int64
	)

	if in.TxFees != nil {
		txFees = in.TxFees.Int64()
	} else {
		txFees = big0.Int64()
	}

	if in.IsUncle {
		blockReward = in.UncleReward.Int64()
		uncleReward = big0.Int64()
	} else {
		blockR, uncleR := in.BlockRewardFunc(in.Block)
		blockReward, uncleReward = blockR.Int64(), uncleR.Int64()
	}

	return txFees, blockReward, uncleReward
}

func processBlockTopics(rawTopics []common.Hash) []string {
	topics := make([]string, len(rawTopics))
	for i, rawTopic := range rawTopics {
		topics[i] = rawTopic.Hex()
	}
	return topics
}

func processBlockLogs(receipt *types.Receipt) []*Log {
	rawLogs := receipt.Logs
	if rawLogs == nil || len(rawLogs) == 0 {
		return make([]*Log, 0)
	}

	var logs []*Log
	for _, rawLog := range rawLogs {
		if rawLog == nil {
			continue
		}

		log := &Log{
			Address: rawLog.Address.Hex(),
			Topics:  processBlockTopics(rawLog.Topics),
			Data:    rawLog.Data,
			Index:   int32(rawLog.Index),
			Removed: rawLog.Removed,
		}

		logs = append(logs, log)
	}

	return logs
}

func processBlockTrace(rawTrace interface{}) *Trace {
	// TODO: Finish implementation
	getTxTransfer := func() []map[string]interface{} {
		var dTraces []map[string]interface{}
		dTraces = append(dTraces, map[string]interface{}{
			"op": "TX",
			//"from":  from.Bytes(),
			//"to":    to,
			//"value": value,
			//"input": input,
		})
		return dTraces
	}

	raw, ok := rawTrace.(map[string]interface{})
	if !ok {
		raw = map[string]interface{}{
			"isError": true,
			"msg":     rawTrace,
		}
	}

	isError := raw["isError"].(bool)
	transfers, ok := raw["transfers"].([]map[string]interface{})
	if !isError && !ok {
		raw["transfers"] = getTxTransfer()
	} else {
		raw["transfers"] = append(transfers, getTxTransfer()[0])
	}

	return &Trace{
		IsError: func() bool {
			return raw["isError"].(bool)
		}(),
		Msg: func() string {
			return raw["msg"].(string)
		}(),
		Transfers: func() []*Transfer {
			return make([]*Transfer, 0)
		}(),
	}
}

func processBlockTxs(state *state.StateDB, in *BlockIn) []*Transaction {
	if in == nil {
		return make([]*Transaction, 0)
	}

	var txs []*Transaction
	blockTxs := in.BlockTxs
	for i, blockTx := range *blockTxs {
		header := in.Block.Header()
		rawTx := blockTx.Tx
		receipt := blockTx.Receipt
		if receipt == nil {
			continue
		}
		signer := in.Signer
		from, _ := types.Sender(signer, rawTx)
		_v, _r, _s := rawTx.RawSignatureValues()
		fromBalance := state.GetBalance(from)
		to := func() UnionNullString {
			if rawTx.To() == nil {
				return UnionNullString{
					UnionType: UnionNullStringTypeEnumNull,
				}
			}
			to := rawTx.To().Hex()
			return UnionNullString{
				String:    to,
				UnionType: UnionNullStringTypeEnumString,
			}
		}()
		toBalance := func() UnionNullLong {
			if rawTx.To() == nil {
				return UnionNullLong{
					UnionType: UnionNullLongTypeEnumNull,
				}
			}
			toBalance := state.GetBalance(*rawTx.To()).Int64()
			return UnionNullLong{
				Long:      toBalance,
				UnionType: UnionNullLongTypeEnumLong,
			}
		}
		value := rawTx.Value()
		input := rawTx.Data()
		contractAddress := func() UnionNullString {
			if receipt.ContractAddress == (common.Address{}) {
				return UnionNullString{
					UnionType: UnionNullStringTypeEnumNull,
				}
			}
			return UnionNullString{
				String:    receipt.ContractAddress.Hex(),
				UnionType: UnionNullStringTypeEnumString,
			}
		}()

		tx := &Transaction{
			Hash:              rawTx.Hash().Hex(),
			Root:              header.ReceiptHash.Hex(),
			Index:             int32(i),
			Timestamp:         blockTx.Timestamp.Int64(),
			Nonce:             int64(rawTx.Nonce()),
			NonceHash:         crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(rawTx.Nonce())).Bytes()).Hex(),
			From:              from.Hex(),
			FromBalance:       fromBalance.Int64(),
			To:                to,
			ToBalance:         toBalance(),
			Input:             input,
			Gas:               int64(rawTx.Gas()),
			GasPrice:          rawTx.GasPrice().Int64(),
			GasUsed:           int64(receipt.GasUsed),
			CumulativeGasUsed: int64(receipt.CumulativeGasUsed),
			ContractAddress:   contractAddress,
			LogsBloom:         receipt.Bloom.Bytes(),
			Value:             value.Int64(),
			R:                 hexutil.Encode(_r.Bytes()),
			V:                 hexutil.Encode(_v.Bytes()),
			S:                 hexutil.Encode(_s.Bytes()),
			Status:            int64(receipt.Status),
			Logs:              processBlockLogs(receipt),
			Trace:             processBlockTrace(blockTx.Trace),
		}

		txs = append(txs, tx)
	}

	return txs
}

func processInsertionPendingTxs(state *state.StateDB, rawPTxs []*PendingTxIn) PendingTxs {
	var pTxs []*PendingTx

	for _, raw := range rawPTxs {
		tx := raw.Tx
		from, _ := types.Sender(raw.Signer, raw.Tx)
		fromBalance := state.GetBalance(from)
		to := func() UnionNullString {
			if tx.To() == nil {
				return UnionNullString{
					UnionType: UnionNullStringTypeEnumNull,
				}
			}
			to := tx.To().Hex()
			return UnionNullString{
				String:    to,
				UnionType: UnionNullStringTypeEnumString,
			}
		}()
		toBalance := func() UnionNullLong {
			if tx.To() == nil {
				return UnionNullLong{
					UnionType: UnionNullLongTypeEnumNull,
				}
			}
			toBalance := state.GetBalance(*tx.To()).Int64()
			return UnionNullLong{
				Long:      toBalance,
				UnionType: UnionNullLongTypeEnumLong,
			}
		}
		contractAddress := func() UnionNullString {
			if raw.Receipt.ContractAddress == (common.Address{}) {
				return UnionNullString{
					UnionType: UnionNullStringTypeEnumNull,
				}
			}
			return UnionNullString{
				String:    raw.Receipt.ContractAddress.Hex(),
				UnionType: UnionNullStringTypeEnumString,
			}
		}()
		input := tx.Data()
		value := tx.Value()
		_v, _r, _s := tx.RawSignatureValues()

		pTx := &PendingTx{
			Hash:              tx.Hash().Hex(),
			Nonce:             int64(tx.Nonce()),
			NonceHash:         crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(tx.Nonce())).Bytes()).Hex(),
			From:              from.Hex(),
			FromBalance:       fromBalance.Int64(),
			To:                to,
			ToBalance:         toBalance(),
			Input:             input,
			Gas:               int64(tx.Gas()),
			GasPrice:          tx.GasPrice().Int64(),
			GasUsed:           int64(raw.Receipt.GasUsed),
			CumulativeGasUsed: int64(raw.Receipt.CumulativeGasUsed),
			ContractAddress:   contractAddress,
			LogsBloom:         raw.Receipt.Bloom.Bytes(),
			Value:             value.Int64(),
			R:                 hexutil.Encode(_r.Bytes()),
			V:                 hexutil.Encode(_v.Bytes()),
			S:                 hexutil.Encode(_s.Bytes()),
			Status:            int64(raw.Receipt.Status),
			Logs:              processBlockLogs(raw.Receipt),
			Trace:             processBlockTrace(raw.Trace),
			TxStatus:          raw.Action,
		}
		pTxs = append(pTxs, pTx)
	}

	return PendingTxs{
		Transactions: pTxs,
	}
}

func processDeletionPendingTxs(rawPTxs []*PendingTxIn) PendingTxs {
	var pTxs []*PendingTx

	for _, raw := range rawPTxs {
		pTx := &PendingTx{
			Hash:     raw.Tx.Hash().Hex(),
			TxStatus: raw.Action,
		}
		pTxs = append(pTxs, pTx)
	}

	return PendingTxs{
		Transactions: pTxs,
	}
}
