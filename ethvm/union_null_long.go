// Code generated by github.com/actgardner/gogen-avro. DO NOT EDIT.
/*
 * SOURCE:
 *     schemas.v1.kafka.asvc
 */

package ethvm

type UnionNullLong struct {
	Null      interface{}
	Long      int64
	UnionType UnionNullLongTypeEnum
}

type UnionNullLongTypeEnum int

const (
	UnionNullLongTypeEnumNull UnionNullLongTypeEnum = 0
	UnionNullLongTypeEnumLong UnionNullLongTypeEnum = 1
)
