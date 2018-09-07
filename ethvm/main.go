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
	"github.com/ethereum/go-ethereum/ethvm/models"
	"github.com/ethereum/go-ethereum/ethvm/registry"
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

	// EthVMSchemaRegistryFlag URL of the kafka schema registry
	EthVMSchemaRegistryFlag = cli.StringFlag{
		Name:  "ethvm-kafka-schema-registry-url",
		Usage: "URL of the schema registry",
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

func (in *BlockIn) bytes(schemaId int, state *state.StateDB) []byte {
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
	uncles := func() []string {
		uncles := make([]string, len(block.Uncles()))
		for i, uncle := range block.Uncles() {
			uncles[i] = uncle.Hash().Hex()
		}
		return uncles
	}()

	b := &models.Block{
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
		Uncles:           uncles,
		BlockReward:      blockReward,
		UncleReward:      uncleReward,
	}

	var buf bytes.Buffer
	b.Serialize(&buf)

	return toAvroBytes(schemaId, buf.Bytes())
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
	Action  models.Action
}

func pendingTxBytes(schemaId int, ptx models.PendingTx) []byte {
	// Encode data
	var buf bytes.Buffer
	ptx.Serialize(&buf)

	return toAvroBytes(2, buf.Bytes())
}

func NewPendingTxIn(tx *types.Transaction, trace interface{}, signer types.Signer, receipt *types.Receipt, action models.Action) *PendingTxIn {
	return &PendingTxIn{
		Tx:      tx,
		Trace:   trace,
		Signer:  signer,
		Receipt: receipt,
		Action:  action,
	}
}

func UpdatePendingTxIn(tx *types.Transaction, action models.Action) *PendingTxIn {
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
	brokers              string
	schemaRegistryClient registry.SchemaRegistryClient

	// Kafka - Topics
	blocksTopic string
	pTxsTopic   string

	// Kafka - Schemas ids
	blocksSchemaId int
	pTxsSchemaId   int

	// Kafka - Producers
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
		schemaRegistryClient: func() registry.SchemaRegistryClient {
			url := "http://localhost:8081"
			if ctx.GlobalString(EthVMSchemaRegistryFlag.Name) != "" {
				url = ctx.GlobalString(EthVMSchemaRegistryFlag.Name)
			}
			return registry.NewSchemaRegistryClient([]string{url})
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

	var err error

	// Retrieve schemas ids
	blocksIds, err := e.schemaRegistryClient.GetVersions(e.blocksTopic + "-value")
	if err != nil {
		panic(err)
	}
	e.blocksSchemaId = blocksIds[0]

	pTxsIds, err := e.schemaRegistryClient.GetVersions(e.pTxsTopic + "-value")
	if err != nil {
		panic(err)
	}
	e.pTxsSchemaId = pTxsIds[0]

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

// ProcessBlock Adds a new Block to EthVM
func (e *EthVM) ProcessBlock(state *state.StateDB, blockIn *BlockIn) {
	if !e.isEnabled() {
		return
	}

	// Send to Kafka
	err := e.blocksW.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(hexutil.Encode(blockIn.Block.Header().Hash().Bytes())),
		Value: blockIn.bytes(e.blocksSchemaId, state),
	})
	if err != nil {
		panic(err)
	}
}

// ProcessPendingTx Validates and store pending tx into DB
func (e *EthVM) ProcessPendingTx(state *state.StateDB, pTx *PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(pTx.Tx.Hash().Hex()),
		Value: pendingTxBytes(e.pTxsSchemaId, processPendingTx(state, pTx)),
	})
	if err != nil {
		panic(err)
	}
}

// ProcessPendingTxs Validates and store pending txs into DB
func (e *EthVM) ProcessPendingTxs(state *state.StateDB, pTxs []*PendingTxIn) {
	if !e.isEnabled() {
		return
	}

	// Send to kafka
	for _, pTx := range pTxs {
		err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(pTx.Tx.Hash().Hex()),
			Value: pendingTxBytes(e.pTxsSchemaId, processPendingTx(state, pTx)),
		})
		if err != nil {
			panic(err)
		}
	}
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

func processBlockLogs(receipt *types.Receipt) []*models.Log {
	rawLogs := receipt.Logs
	if rawLogs == nil || len(rawLogs) == 0 {
		return make([]*models.Log, 0)
	}

	var logs []*models.Log
	for _, rawLog := range rawLogs {
		if rawLog == nil {
			continue
		}

		log := &models.Log{
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

func processBlockTrace(rawTrace interface{}) *models.Trace {
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

	return &models.Trace{
		IsError: func() bool {
			return raw["isError"].(bool)
		}(),
		Msg: func() string {
			return raw["msg"].(string)
		}(),
		Transfers: func() []*models.Transfer {
			return make([]*models.Transfer, 0)
		}(),
	}
}

func processBlockTxs(state *state.StateDB, in *BlockIn) []*models.Transaction {
	if in == nil {
		return make([]*models.Transaction, 0)
	}

	var txs []*models.Transaction
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
		to := func() models.UnionNullString {
			if rawTx.To() == nil {
				return models.UnionNullString{
					UnionType: models.UnionNullStringTypeEnumNull,
				}
			}
			to := rawTx.To().Hex()
			return models.UnionNullString{
				String:    to,
				UnionType: models.UnionNullStringTypeEnumString,
			}
		}()
		toBalance := func() models.UnionNullLong {
			if rawTx.To() == nil {
				return models.UnionNullLong{
					UnionType: models.UnionNullLongTypeEnumNull,
				}
			}
			toBalance := state.GetBalance(*rawTx.To()).Int64()
			return models.UnionNullLong{
				Long:      toBalance,
				UnionType: models.UnionNullLongTypeEnumLong,
			}
		}
		value := rawTx.Value()
		input := rawTx.Data()
		contractAddress := func() models.UnionNullString {
			if receipt.ContractAddress == (common.Address{}) {
				return models.UnionNullString{
					UnionType: models.UnionNullStringTypeEnumNull,
				}
			}
			return models.UnionNullString{
				String:    receipt.ContractAddress.Hex(),
				UnionType: models.UnionNullStringTypeEnumString,
			}
		}()

		tx := &models.Transaction{
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

func processPendingTx(state *state.StateDB, raw *PendingTxIn) models.PendingTx {
	if state == nil {
		return processSimplePendingTxs(raw)
	}

	tx := raw.Tx
	from, _ := types.Sender(raw.Signer, raw.Tx)
	fromBalance := state.GetBalance(from)
	to := func() models.UnionNullString {
		if tx.To() == nil {
			return models.UnionNullString{
				UnionType: models.UnionNullStringTypeEnumNull,
			}
		}
		to := tx.To().Hex()
		return models.UnionNullString{
			String:    to,
			UnionType: models.UnionNullStringTypeEnumString,
		}
	}()
	toBalance := func() models.UnionNullLong {
		if tx.To() == nil {
			return models.UnionNullLong{
				UnionType: models.UnionNullLongTypeEnumNull,
			}
		}
		toBalance := state.GetBalance(*tx.To()).Int64()
		return models.UnionNullLong{
			Long:      toBalance,
			UnionType: models.UnionNullLongTypeEnumLong,
		}
	}
	contractAddress := func() models.UnionNullString {
		if raw.Receipt.ContractAddress == (common.Address{}) {
			return models.UnionNullString{
				UnionType: models.UnionNullStringTypeEnumNull,
			}
		}
		return models.UnionNullString{
			String:    raw.Receipt.ContractAddress.Hex(),
			UnionType: models.UnionNullStringTypeEnumString,
		}
	}()
	input := tx.Data()
	value := tx.Value()
	_v, _r, _s := tx.RawSignatureValues()

	pTx := models.PendingTx{
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

	return pTx
}

func processSimplePendingTxs(raw *PendingTxIn) models.PendingTx {
	pTx := models.PendingTx{
		Hash:     raw.Tx.Hash().Hex(),
		TxStatus: raw.Action,
	}
	return pTx
}

func toAvroBytes(id int, data []byte) []byte {
	// Encode data per kafka / avro spec
	buffer := &bytes.Buffer{}

	// 1) Magic bytes
	_, err := buffer.Write([]byte{0})
	if err != nil {
		panic(err)
	}

	// 2) Id
	idSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(idSlice, uint32(id))
	_, err = buffer.Write(idSlice)
	if err != nil {
		panic(err)
	}

	// 3) Add data
	buffer.Write(data)

	return buffer.Bytes()
}
