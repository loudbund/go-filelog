package filelog_v1

import (
	"bytes"
	"encoding/binary"
)

func utilsEncodeUDiskIndex(Data *UDiskIndex) []byte {
	b := bytes.NewBuffer([]byte{})
	b.Write(utilsInt16ToBytes(Data.DataFileIndex)) // 索引文件序号
	b.Write(utilsInt64ToBytes(Data.DataOffset))    // 存储文件偏移量
	b.Write(utilsInt16ToBytes(Data.DataType))      // 数据内容类型
	return b.Bytes()
}
func utilsDecodeUDiskIndex(Bytes []byte) *UDiskIndex {
	return &UDiskIndex{
		DataFileIndex: int16(binary.BigEndian.Uint16(Bytes[:2])),
		DataOffset:    int64(binary.BigEndian.Uint16(Bytes[2:10])),
		DataType:      int16(binary.BigEndian.Uint16(Bytes[10:12])),
	}
}
func utilsEncodeUDiskData(Data *UDiskData) []byte {
	b := bytes.NewBuffer([]byte{})
	b.Write(utilsInt16ToBytes(Data.DataStart))  // 索引文件序号
	b.Write(utilsInt32ToBytes(Data.DataLength)) // 存储文件偏移量
	b.Write(Data.Data)                          // 数据内容类型
	return b.Bytes()
}
func utilsDecodeUDiskData(Bytes []byte) *UDiskData {
	return &UDiskData{
		DataStart:  int16(binary.BigEndian.Uint16(Bytes[:2])),
		DataLength: int32(binary.BigEndian.Uint16(Bytes[2:6])),
		Data:       Bytes[6:],
	}
}

// 数字类型转换1： int转[]byte
func utilsInt2Bytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// 数字类型转换2： int16转[]byte
func utilsInt16ToBytes(i int16) []byte {
	var buf = make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}

// 数字类型转换3： int32转[]byte
func utilsInt32ToBytes(i int32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

// 数字类型转换4： int64转[]byte
func utilsInt64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// 数字类型转换5： []byte转int
func utilsBytes2Int(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	_ = binary.Read(bytesBuffer, binary.BigEndian, &x)
	binary.BigEndian.Uint32(b)

	return int(x)
}

// 数字类型转换6： []byte转int16
func utilsBytes2Int16(buf []byte) int16 {
	return int16(binary.BigEndian.Uint16(buf))
}

// 数字类型转换7： []byte转int32
func utilsBytes2Int32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

// 数字类型转换8： []byte转int64
func utilsBytes2Int64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
