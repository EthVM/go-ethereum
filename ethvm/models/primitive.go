// Code generated by github.com/actgardner/gogen-avro. DO NOT EDIT.
/*
 * SOURCES:
 *     block.schema.v1.asvc
 *     pendingtx.schema.v1.asvc
 */

package models

import (
	"fmt"
	"io"
)

type ByteReader interface {
	ReadByte() (byte, error)
}

type ByteWriter interface {
	Grow(int)
	WriteByte(byte) error
}

func encodeInt(w io.Writer, byteCount int, encoded uint64) error {
	var err error
	var bb []byte
	bw, ok := w.(ByteWriter)
	// To avoid reallocations, grow capacity to the largest possible size
	// for this integer
	if ok {
		bw.Grow(byteCount)
	} else {
		bb = make([]byte, 0, byteCount)
	}

	if encoded == 0 {
		if bw != nil {
			err = bw.WriteByte(0)
			if err != nil {
				return err
			}
		} else {
			bb = append(bb, byte(0))
		}
	} else {
		for encoded > 0 {
			b := byte(encoded & 127)
			encoded = encoded >> 7
			if !(encoded == 0) {
				b |= 128
			}
			if bw != nil {
				err = bw.WriteByte(b)
				if err != nil {
					return err
				}
			} else {
				bb = append(bb, b)
			}
		}
	}
	if bw == nil {
		_, err := w.Write(bb)
		return err
	}
	return nil

}

func readAction(r io.Reader) (Action, error) {
	val, err := readInt(r)
	return Action(val), err
}

func readArrayBytes(r io.Reader) ([][]byte, error) {
	var err error
	var blkSize int64
	var arr = make([][]byte, 0)
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readBytes(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func readArrayLog(r io.Reader) ([]*Log, error) {
	var err error
	var blkSize int64
	var arr = make([]*Log, 0)
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readLog(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func readArrayTransaction(r io.Reader) ([]*Transaction, error) {
	var err error
	var blkSize int64
	var arr = make([]*Transaction, 0)
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readTransaction(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func readArrayTransfer(r io.Reader) ([]*Transfer, error) {
	var err error
	var blkSize int64
	var arr = make([]*Transfer, 0)
	for {
		blkSize, err = readLong(r)
		if err != nil {
			return nil, err
		}
		if blkSize == 0 {
			break
		}
		if blkSize < 0 {
			blkSize = -blkSize
			_, err = readLong(r)
			if err != nil {
				return nil, err
			}
		}
		for i := int64(0); i < blkSize; i++ {
			elem, err := readTransfer(r)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
	}
	return arr, nil
}

func readBlock(r io.Reader) (*Block, error) {
	var str = &Block{}
	var err error
	str.Number, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Hash, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.ParentHash, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Uncle, err = readBool(r)
	if err != nil {
		return nil, err
	}
	str.Status, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.Timestamp, err = readLong(r)
	if err != nil {
		return nil, err
	}
	str.Nonce, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.MixDigest, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Sha3Uncles, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.LogsBloom, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.StateRoot, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.TransactionsRoot, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Miner, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Difficulty, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.TotalDifficulty, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.ExtraData, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Size, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasLimit, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasUsed, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.TxsFees, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.BlockReward, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.UncleReward, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Stats, err = readUnionNullBlockStats(r)
	if err != nil {
		return nil, err
	}
	str.Transactions, err = readArrayTransaction(r)
	if err != nil {
		return nil, err
	}
	str.Uncles, err = readArrayBytes(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readBlockStats(r io.Reader) (*BlockStats, error) {
	var str = &BlockStats{}
	var err error
	str.BlockTimeMs, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.NumFailedTxs, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.NumSuccessfulTxs, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.AvgGasPrice, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.AvgTxsFees, err = readBytes(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readBool(r io.Reader) (bool, error) {
	var b byte
	var err error
	if br, ok := r.(ByteReader); ok {
		b, err = br.ReadByte()
	} else {
		bs := make([]byte, 1)
		_, err = io.ReadFull(r, bs)
		if err != nil {
			return false, err
		}
		b = bs[0]
	}
	return b == 1, nil
}

func readBytes(r io.Reader) ([]byte, error) {
	size, err := readLong(r)
	if err != nil {
		return nil, err
	}
	bb := make([]byte, size)
	_, err = io.ReadFull(r, bb)
	return bb, err
}

func readInt(r io.Reader) (int32, error) {
	var v int
	buf := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		v |= int(b&127) << shift
		if b&128 == 0 {
			break
		}
	}
	datum := (int32(v>>1) ^ -int32(v&1))
	return datum, nil
}

func readLog(r io.Reader) (*Log, error) {
	var str = &Log{}
	var err error
	str.Address, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Topics, err = readArrayBytes(r)
	if err != nil {
		return nil, err
	}
	str.Data, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Index, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.Removed, err = readBool(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readLong(r io.Reader) (int64, error) {
	var v uint64
	buf := make([]byte, 1)
	for shift := uint(0); ; shift += 7 {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		v |= uint64(b&127) << shift
		if b&128 == 0 {
			break
		}
	}
	datum := (int64(v>>1) ^ -int64(v&1))
	return datum, nil
}

func readNull(_ io.Reader) (interface{}, error) {
	return nil, nil
}

func readPendingTx(r io.Reader) (*PendingTx, error) {
	var str = &PendingTx{}
	var err error
	str.Hash, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Nonce, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.NonceHash, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.From, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.FromBalance, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.To, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.ToBalance, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Input, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.ContractAddress, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Value, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Gas, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasPrice, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasUsed, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.CumulativeGasUsed, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.V, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.R, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.S, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Status, err = readUnionNullInt(r)
	if err != nil {
		return nil, err
	}
	str.LogsBloom, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Logs, err = readArrayLog(r)
	if err != nil {
		return nil, err
	}
	str.Trace, err = readTrace(r)
	if err != nil {
		return nil, err
	}
	str.TxStatus, err = readAction(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readTrace(r io.Reader) (*Trace, error) {
	var str = &Trace{}
	var err error
	str.Error, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.Transfers, err = readArrayTransfer(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readTransaction(r io.Reader) (*Transaction, error) {
	var str = &Transaction{}
	var err error
	str.Hash, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Root, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Index, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.Timestamp, err = readLong(r)
	if err != nil {
		return nil, err
	}
	str.Nonce, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.NonceHash, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.From, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.FromBalance, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.To, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.ToBalance, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Input, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.ContractAddress, err = readUnionNullBytes(r)
	if err != nil {
		return nil, err
	}
	str.Value, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Gas, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasPrice, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.GasUsed, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.CumulativeGasUsed, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.V, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.R, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.S, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Status, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.LogsBloom, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Logs, err = readArrayLog(r)
	if err != nil {
		return nil, err
	}
	str.Trace, err = readTrace(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readTransfer(r io.Reader) (*Transfer, error) {
	var str = &Transfer{}
	var err error
	str.Op, err = readInt(r)
	if err != nil {
		return nil, err
	}
	str.Value, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.From, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.FromBalance, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.To, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.ToBalance, err = readBytes(r)
	if err != nil {
		return nil, err
	}
	str.Input, err = readBytes(r)
	if err != nil {
		return nil, err
	}

	return str, nil
}

func readUnionNullBlockStats(r io.Reader) (UnionNullBlockStats, error) {
	field, err := readLong(r)
	var unionStr UnionNullBlockStats
	if err != nil {
		return unionStr, err
	}
	unionStr.UnionType = UnionNullBlockStatsTypeEnum(field)
	switch unionStr.UnionType {
	case UnionNullBlockStatsTypeEnumNull:
		val, err := readNull(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.Null = val
	case UnionNullBlockStatsTypeEnumBlockStats:
		val, err := readBlockStats(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.BlockStats = val

	default:
		return unionStr, fmt.Errorf("Invalid value for UnionNullBlockStats")
	}
	return unionStr, nil
}

func readUnionNullBytes(r io.Reader) (UnionNullBytes, error) {
	field, err := readLong(r)
	var unionStr UnionNullBytes
	if err != nil {
		return unionStr, err
	}
	unionStr.UnionType = UnionNullBytesTypeEnum(field)
	switch unionStr.UnionType {
	case UnionNullBytesTypeEnumNull:
		val, err := readNull(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.Null = val
	case UnionNullBytesTypeEnumBytes:
		val, err := readBytes(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.Bytes = val

	default:
		return unionStr, fmt.Errorf("Invalid value for UnionNullBytes")
	}
	return unionStr, nil
}

func readUnionNullInt(r io.Reader) (UnionNullInt, error) {
	field, err := readLong(r)
	var unionStr UnionNullInt
	if err != nil {
		return unionStr, err
	}
	unionStr.UnionType = UnionNullIntTypeEnum(field)
	switch unionStr.UnionType {
	case UnionNullIntTypeEnumNull:
		val, err := readNull(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.Null = val
	case UnionNullIntTypeEnumInt:
		val, err := readInt(r)
		if err != nil {
			return unionStr, err
		}
		unionStr.Int = val

	default:
		return unionStr, fmt.Errorf("Invalid value for UnionNullInt")
	}
	return unionStr, nil
}

func writeAction(r Action, w io.Writer) error {
	return writeInt(int32(r), w)
}

func writeArrayBytes(r [][]byte, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil || len(r) == 0 {
		return err
	}
	for _, e := range r {
		err = writeBytes(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func writeArrayLog(r []*Log, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil || len(r) == 0 {
		return err
	}
	for _, e := range r {
		err = writeLog(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func writeArrayTransaction(r []*Transaction, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil || len(r) == 0 {
		return err
	}
	for _, e := range r {
		err = writeTransaction(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func writeArrayTransfer(r []*Transfer, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil || len(r) == 0 {
		return err
	}
	for _, e := range r {
		err = writeTransfer(e, w)
		if err != nil {
			return err
		}
	}
	return writeLong(0, w)
}

func writeBlock(r *Block, w io.Writer) error {
	var err error
	err = writeBytes(r.Number, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Hash, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.ParentHash, w)
	if err != nil {
		return err
	}
	err = writeBool(r.Uncle, w)
	if err != nil {
		return err
	}
	err = writeInt(r.Status, w)
	if err != nil {
		return err
	}
	err = writeLong(r.Timestamp, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Nonce, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.MixDigest, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Sha3Uncles, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.LogsBloom, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.StateRoot, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.TransactionsRoot, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Miner, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Difficulty, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.TotalDifficulty, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.ExtraData, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Size, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.GasLimit, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.GasUsed, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.TxsFees, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.BlockReward, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.UncleReward, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBlockStats(r.Stats, w)
	if err != nil {
		return err
	}
	err = writeArrayTransaction(r.Transactions, w)
	if err != nil {
		return err
	}
	err = writeArrayBytes(r.Uncles, w)
	if err != nil {
		return err
	}

	return nil
}
func writeBlockStats(r *BlockStats, w io.Writer) error {
	var err error
	err = writeInt(r.BlockTimeMs, w)
	if err != nil {
		return err
	}
	err = writeInt(r.NumFailedTxs, w)
	if err != nil {
		return err
	}
	err = writeInt(r.NumSuccessfulTxs, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.AvgGasPrice, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.AvgTxsFees, w)
	if err != nil {
		return err
	}

	return nil
}

func writeBool(r bool, w io.Writer) error {
	var b byte
	if r {
		b = byte(1)
	}

	var err error
	if bw, ok := w.(ByteWriter); ok {
		err = bw.WriteByte(b)
	} else {
		bb := make([]byte, 1)
		bb[0] = b
		_, err = w.Write(bb)
	}
	if err != nil {
		return err
	}
	return nil
}

func writeBytes(r []byte, w io.Writer) error {
	err := writeLong(int64(len(r)), w)
	if err != nil {
		return err
	}
	_, err = w.Write(r)
	return err
}

func writeInt(r int32, w io.Writer) error {
	downShift := uint32(31)
	encoded := uint64((uint32(r) << 1) ^ uint32(r>>downShift))
	const maxByteSize = 5
	return encodeInt(w, maxByteSize, encoded)
}

func writeLog(r *Log, w io.Writer) error {
	var err error
	err = writeBytes(r.Address, w)
	if err != nil {
		return err
	}
	err = writeArrayBytes(r.Topics, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Data, w)
	if err != nil {
		return err
	}
	err = writeInt(r.Index, w)
	if err != nil {
		return err
	}
	err = writeBool(r.Removed, w)
	if err != nil {
		return err
	}

	return nil
}

func writeLong(r int64, w io.Writer) error {
	downShift := uint64(63)
	encoded := uint64((r << 1) ^ (r >> downShift))
	const maxByteSize = 10
	return encodeInt(w, maxByteSize, encoded)
}

func writeNull(_ interface{}, _ io.Writer) error {
	return nil
}

func writePendingTx(r *PendingTx, w io.Writer) error {
	var err error
	err = writeBytes(r.Hash, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.Nonce, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.NonceHash, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.From, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.FromBalance, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.To, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.ToBalance, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.Input, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.ContractAddress, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.Value, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.Gas, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.GasPrice, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.GasUsed, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.CumulativeGasUsed, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.V, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.R, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.S, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Status, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.LogsBloom, w)
	if err != nil {
		return err
	}
	err = writeArrayLog(r.Logs, w)
	if err != nil {
		return err
	}
	err = writeTrace(r.Trace, w)
	if err != nil {
		return err
	}
	err = writeAction(r.TxStatus, w)
	if err != nil {
		return err
	}

	return nil
}
func writeTrace(r *Trace, w io.Writer) error {
	var err error
	err = writeInt(r.Error, w)
	if err != nil {
		return err
	}
	err = writeArrayTransfer(r.Transfers, w)
	if err != nil {
		return err
	}

	return nil
}
func writeTransaction(r *Transaction, w io.Writer) error {
	var err error
	err = writeBytes(r.Hash, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Root, w)
	if err != nil {
		return err
	}
	err = writeInt(r.Index, w)
	if err != nil {
		return err
	}
	err = writeLong(r.Timestamp, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Nonce, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.NonceHash, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.From, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.FromBalance, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.To, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.ToBalance, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Input, w)
	if err != nil {
		return err
	}
	err = writeUnionNullBytes(r.ContractAddress, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Value, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Gas, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.GasPrice, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.GasUsed, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.CumulativeGasUsed, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.V, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.R, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.S, w)
	if err != nil {
		return err
	}
	err = writeInt(r.Status, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.LogsBloom, w)
	if err != nil {
		return err
	}
	err = writeArrayLog(r.Logs, w)
	if err != nil {
		return err
	}
	err = writeTrace(r.Trace, w)
	if err != nil {
		return err
	}

	return nil
}
func writeTransfer(r *Transfer, w io.Writer) error {
	var err error
	err = writeInt(r.Op, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Value, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.From, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.FromBalance, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.To, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.ToBalance, w)
	if err != nil {
		return err
	}
	err = writeBytes(r.Input, w)
	if err != nil {
		return err
	}

	return nil
}

func writeUnionNullBlockStats(r UnionNullBlockStats, w io.Writer) error {
	err := writeLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullBlockStatsTypeEnumNull:
		return writeNull(r.Null, w)
	case UnionNullBlockStatsTypeEnumBlockStats:
		return writeBlockStats(r.BlockStats, w)

	}
	return fmt.Errorf("Invalid value for UnionNullBlockStats")
}

func writeUnionNullBytes(r UnionNullBytes, w io.Writer) error {
	err := writeLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullBytesTypeEnumNull:
		return writeNull(r.Null, w)
	case UnionNullBytesTypeEnumBytes:
		return writeBytes(r.Bytes, w)

	}
	return fmt.Errorf("Invalid value for UnionNullBytes")
}

func writeUnionNullInt(r UnionNullInt, w io.Writer) error {
	err := writeLong(int64(r.UnionType), w)
	if err != nil {
		return err
	}
	switch r.UnionType {
	case UnionNullIntTypeEnumNull:
		return writeNull(r.Null, w)
	case UnionNullIntTypeEnumInt:
		return writeInt(r.Int, w)

	}
	return fmt.Errorf("Invalid value for UnionNullInt")
}
