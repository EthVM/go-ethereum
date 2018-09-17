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
  "fmt"
  "io"
  "math/big"
  "os"
  "runtime"

  "bytes"
  "context"
  "encoding/binary"
  "github.com/ethereum/go-ethereum/common"
  "github.com/ethereum/go-ethereum/common/hexutil"
  "github.com/ethereum/go-ethereum/core/state"
  "github.com/ethereum/go-ethereum/core/types"
  "github.com/ethereum/go-ethereum/crypto"
  "github.com/ethereum/go-ethereum/ethvm/models"
  "github.com/ethereum/go-ethereum/ethvm/registry"
  "github.com/segmentio/kafka-go"
  "gopkg.in/urfave/cli.v1"
)

const (
  NonStatTy = iota
  CanonStatTy
  SideStatTy
)

const (
  InternalTxsTracer = iota
  NoopTracer
)

const (
  TraceOk = iota
  TraceOutOfGasError
  TraceUnknownError
)

var (
  // EthVMFlag Enables ETHVM to listen on Ethereum data
  EthVMEnabledFlag = cli.BoolFlag{
    Name:  "ethvm",
    Usage: "Enables EthVM to listen every data produced on this node",
  }

  // EthVMBrokersFlag Specifies a list of kafka brokers to connect
  EthVMBrokersFlag = cli.StringFlag{
    Name:  "ethvm-brokers",
    Usage: "Specifies a list of kafka brokers to connect",
  }

  // EthVMEnableBlocksTopicFlag Ignores writing block information to Kafka
  EthVMIgnoreBlocksTopicFlag = cli.BoolFlag{
    Name:  "ethvm-ignore-processing-blocks",
    Usage: "Ignores writing block information to Kafka",
  }

  // EthVMBlocksTopicFlag Name of the kafka block topic
  EthVMBlocksTopicFlag = cli.StringFlag{
    Name:  "ethvm-blocks-topic",
    Usage: "Name of the kafka block topic",
  }

  // EthVMEnableBlocksTopicFlag Ignores writing pending txs information to Kafka
  EthVMIgnorePendingTxsTopicFlag = cli.BoolFlag{
    Name:  "ethvm-ignore-processing-pending-txs",
    Usage: "Ignores writing pending txs information to Kafka",
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

  // EthVMTracer Name of the tracer to trace transactions on the EVM
  EthVMTracerFlag = cli.StringFlag{
    Name:  "ethvm-tracer",
    Value: "internal-txs-tracer",
    Usage: "Name of the tracer to add to EVM (options: internal-txs-tracer, noop-tracer)",
  }

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
  td := func() []byte {
    if in.PrevTd == nil {
      return uintToBytes(big0.Uint64())
    }
    return uintToBytes((new(big.Int).Add(block.Difficulty(), in.PrevTd)).Uint64())
  }()
  txFees, blockReward, uncleReward := calculateBlockReward(in)
  txs := processBlockTxs(state, in)
  uncles := func() [][]byte {
    uncles := make([][]byte, len(block.Uncles()))
    for i, uncle := range block.Uncles() {
      uncles[i] = uncle.Hash().Bytes()
    }
    return uncles
  }()
  stats := models.UnionNullBlockStats{
    UnionType: models.UnionNullBlockStatsTypeEnumNull,
  }

  b := &models.Block{
    Number:           header.Number.Bytes(),
    Hash:             header.Hash().Bytes(),
    ParentHash:       header.Hash().Bytes(),
    MixDigest:        header.MixDigest.Bytes(),
    Uncle:            in.IsUncle,
    Status:           int32(in.Status),
    Timestamp:        header.Time.Int64() * 1000,
    Nonce:            uintToBytes(header.Nonce.Uint64()),
    Sha3Uncles:       header.UncleHash.Bytes(),
    LogsBloom:        header.Bloom.Bytes(),
    StateRoot:        header.Root.Bytes(),
    TransactionsRoot: header.ReceiptHash.Bytes(),
    Miner:            header.Coinbase.Bytes(),
    Difficulty:       header.Difficulty.Bytes(),
    TotalDifficulty:  td,
    ExtraData:        header.Extra,
    Size:             uintToBytes(uint64(block.Size())),
    GasLimit:         uintToBytes(header.GasLimit),
    GasUsed:          uintToBytes(header.GasUsed),
    Transactions:     txs,
    TxsFees:          txFees,
    Uncles:           uncles,
    BlockReward:      blockReward,
    UncleReward:      uncleReward,
    Stats:            stats,
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
      return big0
    },
    Status: status,
  }
}

type PendingTxIn struct {
  Tx      *types.Transaction
  Trace   map[string]interface{}
  Signer  types.Signer
  Receipt *types.Receipt
  Action  models.Action
}

func pendingTxBytes(schemaId int, ptx models.PendingTx) []byte {
  // Encode data
  var buf bytes.Buffer
  ptx.Serialize(&buf)

  return toAvroBytes(schemaId, buf.Bytes())
}

func NewPendingTxIn(tx *types.Transaction, trace map[string]interface{}, signer types.Signer, receipt *types.Receipt, action models.Action) *PendingTxIn {
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
  Trace     map[string]interface{}
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

  // Kafka - Ignore
  ignoreProcessingBlocks     bool
  ignoreProcessingPendingTxs bool

  // Kafka - Topics
  blocksTopic string
  pTxsTopic   string

  // Kafka - Schemas ids
  blocksSchemaId int
  pTxsSchemaId   int

  // Kafka - Producers
  blocksW *kafka.Writer
  pTxsW   *kafka.Writer

  // Tracer
  tracer int
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
    enabled: ctx.GlobalBool(EthVMEnabledFlag.Name),
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
    ignoreProcessingBlocks: func() bool {
      return ctx.GlobalBool(EthVMIgnoreBlocksTopicFlag.Name)
    }(),
    ignoreProcessingPendingTxs: func() bool {
      return ctx.GlobalBool(EthVMIgnorePendingTxsTopicFlag.Name)
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
    tracer: func() int {
      tracer := InternalTxsTracer
      if ctx.GlobalString(EthVMTracerFlag.Name) != "" {
        rawTracer := ctx.GlobalString(EthVMTracerFlag.Name)
        tracer = toTracer(rawTracer)
      }
      return tracer
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
  if !e.isEnabled() || e.ignoreProcessingBlocks {
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
  if !e.isEnabled() || e.ignoreProcessingPendingTxs {
    return
  }

  go func() {
    err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
      Key:   []byte(pTx.Tx.Hash().Hex()),
      Value: pendingTxBytes(e.pTxsSchemaId, processPendingTx(state, pTx)),
    })
    if err != nil {
      panic(err)
    }
  }()
}

// ProcessPendingTxs Validates and store pending txs into DB
func (e *EthVM) ProcessPendingTxs(state *state.StateDB, pTxs []*PendingTxIn) {
  if !e.isEnabled() || e.ignoreProcessingPendingTxs {
    return
  }

  // Send to kafka
  go func() {
    for _, pTx := range pTxs {
      err := e.pTxsW.WriteMessages(context.Background(), kafka.Message{
        Key:   []byte(pTx.Tx.Hash().Hex()),
        Value: pendingTxBytes(e.pTxsSchemaId, processPendingTx(state, pTx)),
      })
      if err != nil {
        panic(err)
      }
    }
  }()
}

func (e *EthVM) Tracer() int {
  return e.tracer
}

// --------------------
// Helpers
// --------------------

func calculateBlockReward(in *BlockIn) ([]byte, []byte, []byte) {
  var (
    txFees      uint64
    blockReward uint64
    uncleReward uint64
  )

  if in.TxFees != nil {
    txFees = in.TxFees.Uint64()
  } else {
    txFees = big0.Uint64()
  }

  if in.IsUncle {
    blockReward = in.UncleReward.Uint64()
    uncleReward = big0.Uint64()
  } else {
    blockR, uncleR := in.BlockRewardFunc(in.Block)
    blockReward, uncleReward = blockR.Uint64(), uncleR.Uint64()
  }

  return uintToBytes(txFees), uintToBytes(blockReward), uintToBytes(uncleReward)
}

func processBlockTopics(rawTopics []common.Hash) [][]byte {
  topics := make([][]byte, len(rawTopics))
  for i, rawTopic := range rawTopics {
    topics[i] = rawTopic.Bytes()
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
      Address: rawLog.Address.Bytes(),
      Topics:  processBlockTopics(rawLog.Topics),
      Data:    rawLog.Data,
      Index:   int32(rawLog.Index),
      Removed: rawLog.Removed,
    }

    logs = append(logs, log)
  }

  return logs
}

func processBlockTrace(raw map[string]interface{}) *models.Trace {
  return &models.Trace{
    Error: func() int32 {
      return int32(raw["error"].(int))
    }(),
    Transfers: func() []*models.Transfer {
      rawTransfers, _ := raw["transfers"].([]map[string]interface{})
      transfers := make([]*models.Transfer, len(rawTransfers))
      for _, rawTransfer := range rawTransfers {
        transfer := &models.Transfer{
          Op:          int32(rawTransfer["op"].(byte)),
          Value:       rawTransfer["value"].([]byte),
          From:        rawTransfer["from"].([]byte),
          FromBalance: rawTransfer["fromBalance"].([]byte),
          To:          rawTransfer["to"].([]byte),
          ToBalance:   rawTransfer["toBalance"].([]byte),
          Input:       rawTransfer["input"].([]byte),
        }
        transfers = append(transfers, transfer)
      }
      return transfers
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
    to := func() models.UnionNullBytes {
      if rawTx.To() == nil {
        return models.UnionNullBytes{
          UnionType: models.UnionNullBytesTypeEnumNull,
        }
      }
      to := rawTx.To().Bytes()
      return models.UnionNullBytes{
        Bytes:     to,
        UnionType: models.UnionNullBytesTypeEnumBytes,
      }
    }()
    toBalance := func() models.UnionNullBytes {
      if rawTx.To() == nil {
        return models.UnionNullBytes{
          UnionType: models.UnionNullBytesTypeEnumNull,
        }
      }
      toBalance := state.GetBalance(*rawTx.To()).Uint64()
      return models.UnionNullBytes{
        Bytes:     uintToBytes(toBalance),
        UnionType: models.UnionNullBytesTypeEnumBytes,
      }
    }
    value := rawTx.Value()
    input := rawTx.Data()
    contractAddress := func() models.UnionNullBytes {
      if receipt.ContractAddress == (common.Address{}) {
        return models.UnionNullBytes{
          UnionType: models.UnionNullBytesTypeEnumNull,
        }
      }
      return models.UnionNullBytes{
        Bytes:     receipt.ContractAddress.Bytes(),
        UnionType: models.UnionNullBytesTypeEnumBytes,
      }
    }()

    tx := &models.Transaction{
      Hash:              rawTx.Hash().Bytes(),
      Root:              header.ReceiptHash.Bytes(),
      Index:             int32(i),
      Timestamp:         blockTx.Timestamp.Int64(),
      Nonce:             uintToBytes(rawTx.Nonce()),
      NonceHash:         crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(rawTx.Nonce())).Bytes()).Bytes(),
      From:              from.Bytes(),
      FromBalance:       uintToBytes(fromBalance.Uint64()),
      To:                to,
      ToBalance:         toBalance(),
      Input:             input,
      Gas:               uintToBytes(rawTx.Gas()),
      GasPrice:          uintToBytes(rawTx.GasPrice().Uint64()),
      GasUsed:           uintToBytes(receipt.GasUsed),
      CumulativeGasUsed: uintToBytes(receipt.CumulativeGasUsed),
      ContractAddress:   contractAddress,
      LogsBloom:         receipt.Bloom.Bytes(),
      Value:             uintToBytes(value.Uint64()),
      R:                 _r.Bytes(),
      V:                 _v.Bytes(),
      S:                 _s.Bytes(),
      Status:            int32(receipt.Status),
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
  to := func() models.UnionNullBytes {
    if tx.To() == nil {
      return models.UnionNullBytes{
        UnionType: models.UnionNullBytesTypeEnumNull,
      }
    }
    to := tx.To().Bytes()
    return models.UnionNullBytes{
      Bytes:     to,
      UnionType: models.UnionNullBytesTypeEnumBytes,
    }
  }()
  toBalance := func() models.UnionNullBytes {
    if tx.To() == nil {
      return models.UnionNullBytes{
        UnionType: models.UnionNullBytesTypeEnumNull,
      }
    }
    toBalance := uintToBytes(state.GetBalance(*tx.To()).Uint64())
    return models.UnionNullBytes{
      Bytes:     toBalance,
      UnionType: models.UnionNullBytesTypeEnumBytes,
    }
  }
  contractAddress := func() models.UnionNullBytes {
    if raw.Receipt == nil || raw.Receipt.ContractAddress == (common.Address{}) {
      return models.UnionNullBytes{
        UnionType: models.UnionNullBytesTypeEnumNull,
      }
    }
    return models.UnionNullBytes{
      Bytes:     raw.Receipt.ContractAddress.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    }
  }()
  input := tx.Data()
  value := tx.Value()
  _v, _r, _s := tx.RawSignatureValues()

  pTx := models.PendingTx{
    Hash: tx.Hash().Bytes(),
    Nonce: models.UnionNullBytes{
      Bytes:     uintToBytes(tx.Nonce()),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    NonceHash: models.UnionNullBytes{
      Bytes:     crypto.Keccak256Hash(from.Bytes(), big.NewInt(int64(tx.Nonce())).Bytes()).Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    From: models.UnionNullBytes{
      Bytes:     from.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    FromBalance: models.UnionNullBytes{
      Bytes:     uintToBytes(fromBalance.Uint64()),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    To:        to,
    ToBalance: toBalance(),
    Input: models.UnionNullBytes{
      Bytes:     input,
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    Gas: models.UnionNullBytes{
      Bytes:     uintToBytes(tx.Gas()),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    GasPrice: models.UnionNullBytes{
      Bytes:     uintToBytes(tx.GasPrice().Uint64()),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    GasUsed: models.UnionNullBytes{
      Bytes:     uintToBytes(raw.Receipt.GasUsed),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    CumulativeGasUsed: models.UnionNullBytes{
      Bytes:     uintToBytes(raw.Receipt.CumulativeGasUsed),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    ContractAddress: contractAddress,
    LogsBloom: models.UnionNullBytes{
      Bytes:     raw.Receipt.Bloom.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    Value: models.UnionNullBytes{
      Bytes:     uintToBytes(value.Uint64()),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    R: models.UnionNullBytes{
      Bytes:     _r.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    V: models.UnionNullBytes{
      Bytes:     _v.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    S: models.UnionNullBytes{
      Bytes:     _s.Bytes(),
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    Status: models.UnionNullInt{
      Int:       int32(raw.Receipt.Status),
      UnionType: models.UnionNullIntTypeEnumInt,
    },
    Logs:     processBlockLogs(raw.Receipt),
    Trace:    processBlockTrace(raw.Trace),
    TxStatus: raw.Action,
  }

  return pTx
}

func processSimplePendingTxs(raw *PendingTxIn) models.PendingTx {
  pTx := models.PendingTx{
    // Updated data
    Hash:     raw.Tx.Hash().Bytes(),
    TxStatus: raw.Action,

    // Null data
    Nonce: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    NonceHash: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    From: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    FromBalance: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    To: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    ToBalance: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    Input: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    Gas: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    GasPrice: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    GasUsed: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    CumulativeGasUsed: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    ContractAddress: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    LogsBloom: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    Value: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumBytes,
    },
    R: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    V: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    S: models.UnionNullBytes{
      UnionType: models.UnionNullBytesTypeEnumNull,
    },
    Status: models.UnionNullInt{
      UnionType: models.UnionNullIntTypeEnumNull,
    },
    Logs: make([]*models.Log, 0),
    Trace: &models.Trace{
      Error:     TraceOk,
      Transfers: make([]*models.Transfer, 0),
    },
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

func toTracer(s string) int {
  switch s {
  case "noop-tracer":
    return NoopTracer
  case "internal-txs-tracer":
    return InternalTxsTracer
  }
  fatalf("Invalid tracer option specified. Supported options: noop-tracer, internal-txs-tracer.")
  return 0
}

func fatalf(format string, args ...interface{}) {
  w := io.MultiWriter(os.Stdout, os.Stderr)
  if runtime.GOOS == "windows" {
    // The SameFile check below doesn't work on Windows.
    // stdout is unlikely to get redirected though, so just print there.
    w = os.Stdout
  } else {
    outf, _ := os.Stdout.Stat()
    errf, _ := os.Stderr.Stat()
    if outf != nil && errf != nil && os.SameFile(outf, errf) {
      w = os.Stderr
    }
  }
  fmt.Fprintf(w, "Fatal: "+format+"\n", args...)
  os.Exit(1)
}

func uintToBytes(raw uint64) []byte {
  b := make([]byte, 8)
  binary.BigEndian.PutUint64(b, raw)
  return b
}
