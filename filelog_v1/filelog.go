package filelog_v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// 结构体1: 读取内容的结构体
type UData struct {
	Id         int64  // 流水号
	DataType   int16  // 数据类型
	DataLength int32  // 内容长度
	DataOffset int64  // 相对存储文件偏移量
	Data       []byte // 内容
}

// 结构体2： 日志操作结构体
type CFileLog struct {
	AutoId  int64            // 流水id，从0开始
	indexFp *os.File         // 索引文件句柄
	dataFps map[int]*os.File // 日志数据文件句柄数组，键值为数据文件编号
	dataPos map[int]int64    // 日志数据文件位置数组，键值为数据文件编号

	date        string // 日期
	folderLog   string // 日志文件所在目录
	rowsPerFile int64  // 单个数据文件最大行数
}

// 函数1：获取日志操作实例
func New(Folder string, Date string) *CFileLog {
	return &CFileLog{
		folderLog:   Folder,
		date:        Date,
		indexFp:     nil,
		dataFps:     map[int]*os.File{},
		dataPos:     map[int]int64{},
		AutoId:      -1,
		rowsPerFile: 1000000,
	}
}

// 函数2：关闭释放
func (Me *CFileLog) Close() {
	if Me.indexFp != nil {
		_ = Me.indexFp.Close()
	}
	for _, v := range Me.dataFps {
		_ = v.Close()
	}
	Me.indexFp = nil
	Me.dataFps = nil
	Me.AutoId = -2
}

// 函数3：读取位置
func (Me *CFileLog) GetAutoId() (int64, error) {
	if Me.AutoId == -2 {
		return -1, errors.New("实例已关闭")
	}
	// 还没有位置，需要从文件读取
	if Me.AutoId == -1 {
		if err := Me.initAutoId(); err != nil {
			return -1, err
		}
	}
	return Me.AutoId, nil
}

// 函数4：新增一条日志
func (Me *CFileLog) Add(DataType int16, Data []byte) (int64, error) {
	if Me.AutoId == -2 {
		return -1, errors.New("实例已关闭")
	}

	var err error
	// AutoId变量判断处理
	if Me.AutoId == -1 {
		err = Me.initAutoId()
		fmt.Println(Me.AutoId)
		if err != nil {
			return -1, err
		}
	}

	// 索引文件句柄判断处理
	if Me.indexFp == nil {
		if err := Me.initFpIndex(); err != nil {
			return -1, err
		}
	}

	// 内容文件文件序号
	KeyDataFileIndex := int(Me.AutoId / Me.rowsPerFile)
	if _, ok := Me.dataFps[KeyDataFileIndex]; !ok {
		if err := Me.initFpData(KeyDataFileIndex); err != nil {
			return -1, err
		}
	}

	// 内容长度
	KeyDataLen := len(Data)

	// 写入索引
	if _, err = Me.indexFp.Seek(Me.AutoId*14, 0); err != nil {
		return -1, err
	} else {
		b := bytes.NewBuffer([]byte{})
		b.Write(Me.int16ToBytes(DataType))                     // 数据内容类型
		b.Write(Me.int32ToBytes(int32(KeyDataLen)))            // 数据长度
		b.Write(Me.int64ToBytes(Me.dataPos[KeyDataFileIndex])) // 存储文件偏移量
		if _, err := Me.indexFp.Write(b.Bytes()); err != nil {
			return -1, err
		}
	}

	// 写数据
	if _, err = Me.dataFps[KeyDataFileIndex].Seek(Me.dataPos[KeyDataFileIndex], 0); err != nil {
		return -1, err
	} else {
		if _, err := Me.dataFps[KeyDataFileIndex].Write(Data); err != nil {
			return -1, err
		}
		Me.dataPos[KeyDataFileIndex] += int64(KeyDataLen)
	}

	Me.AutoId++

	return Me.AutoId, nil
}

// 函数5：读取一条数据
func (Me *CFileLog) GetOne(Id int64) (*UData, error) {
	if Me.AutoId == -2 {
		return nil, errors.New("实例已关闭")
	}

	// 索引文件句柄判断
	if Me.indexFp == nil {
		if err := Me.initFpIndex(); err != nil {
			return nil, err
		}
	}

	// 获取索引
	var KeyDataType int16
	var KeyDataLength int32
	var KeyOffset int64
	if _, err := Me.indexFp.Seek(Id*14, 0); err != nil {
		return nil, errors.New("索引文件指针移位失败+" + err.Error())
	} else {
		buff, err := Me.fileReadLength(Me.indexFp, 14)
		if err != nil {
			if err.Error() == "EOF" {
				return nil, nil
			} else {
				return nil, errors.New("索引文件内容读取失败+" + err.Error())
			}
		}
		KeyDataType = int16(binary.BigEndian.Uint16(buff[:2]))
		KeyDataLength = int32(binary.BigEndian.Uint32(buff[2:6]))
		KeyOffset = int64(binary.BigEndian.Uint64(buff[6:14]))
	}

	// 内容文件句柄
	var KeyDataBuff []byte
	if true {
		fN := int(Id / Me.rowsPerFile)
		if _, ok := Me.dataFps[fN]; !ok {
			if err := Me.initFpData(fN); err != nil {
				return nil, errors.New("内容文件初始化获取失败+" + err.Error())
			}
		}
		// 读取文件内容
		if _, err := Me.dataFps[fN].Seek(KeyOffset, 0); err != nil {
			return nil, errors.New("内容文件指针移位失败+" + err.Error())
		}
		buff, err := Me.fileReadLength(Me.dataFps[fN], int(KeyDataLength))
		if err != nil {
			return nil, errors.New("内容文件内容读取失败+" + err.Error())
		}
		KeyDataBuff = buff
	}

	return &UData{
		Id:         Id,
		DataType:   KeyDataType,
		DataLength: KeyDataLength,
		DataOffset: KeyOffset,
		Data:       KeyDataBuff,
	}, nil
}

// 辅助函数1：初始化内容文件句柄
func (Me *CFileLog) initFpData(index int) error {
	// 判断日期目录是否存在,没有将会创建
	folderDate, err := Me.getLogDateFolder()
	if err != nil {
		return err
	}

	// 打开内容文件
	f := folderDate + "/data" + strings.Repeat("0", 6-len(strconv.Itoa(index))) + strconv.Itoa(index)
	fp, err := os.OpenFile(f, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644) // 打开文件
	if err != nil {
		return err
	}
	Me.dataFps[index] = fp

	fi, _ := fp.Stat()
	Me.dataPos[index] = fi.Size()

	return nil
}

// 辅助函数2：初始化自增id
func (Me *CFileLog) initAutoId() error {
	if err := Me.initFpIndex(); err != nil {
		return err
	}
	fi, _ := Me.indexFp.Stat()
	Me.AutoId = fi.Size() / 14

	return nil
}

// 辅助函数3：初始化索引文件句柄
func (Me *CFileLog) initFpIndex() error {
	if Me.indexFp == nil {
		// 判断日期目录是否存在
		folderDate, err := Me.getLogDateFolder()
		if err != nil {
			return err
		}

		// 打开文件
		f := folderDate + "/index"
		fpIndex, err := os.OpenFile(f, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644) // 打开文件
		if err != nil {
			return err
		}
		Me.indexFp = fpIndex
	}
	return nil
}

// 辅助函数4：获取日志文件夹
func (Me *CFileLog) getLogDateFolder() (string, error) {
	folderDate := strings.TrimRight(Me.folderLog, "/") + "/" + strings.ReplaceAll(Me.date, "-", "")
	if _, err := os.Stat(folderDate); os.IsNotExist(err) {
		// 创建目录
		err = os.MkdirAll(folderDate, os.ModePerm)
		if err != nil {
			return "", err
		}
	}
	return folderDate, nil
}

// 辅助函数5：读取指定长度数据
func (Me *CFileLog) fileReadLength(fp *os.File, Length int) ([]byte, error) {
	// 读取一批
	var retBuff = make([]byte, 0)
	for {
		ContentTran := make([]byte, Length) // 建立一个slice
		// 读取数据
		if n, err := fp.Read(ContentTran); err == nil {
			if n != Length {
				retBuff = append(retBuff, ContentTran[:n]...)
				Length -= n
			} else {
				retBuff = append(retBuff, ContentTran[:n]...)
				break
			}
		} else {
			return nil, err
		}
	}
	return retBuff, nil
}

// 数字类型转换1： int转[]byte
func (Me *CFileLog) int2Bytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	_ = binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

// 数字类型转换2： int16转[]byte
func (Me *CFileLog) int16ToBytes(i int16) []byte {
	var buf = make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}

// 数字类型转换3： int32转[]byte
func (Me *CFileLog) int32ToBytes(i int32) []byte {
	var buf = make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

// 数字类型转换4： int64转[]byte
func (Me *CFileLog) int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// 数字类型转换5： []byte转int
func (Me *CFileLog) bytes2Int(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	_ = binary.Read(bytesBuffer, binary.BigEndian, &x)
	binary.BigEndian.Uint32(b)

	return int(x)
}

// 数字类型转换6： []byte转int16
func (Me *CFileLog) bytes2Int16(buf []byte) int16 {
	return int16(binary.BigEndian.Uint16(buf))
}

// 数字类型转换7： []byte转int32
func (Me *CFileLog) bytes2Int32(buf []byte) int32 {
	return int32(binary.BigEndian.Uint32(buf))
}

// 数字类型转换8： []byte转int64
func (Me *CFileLog) bytes2Int64(buf []byte) int64 {
	return int64(binary.BigEndian.Uint64(buf))
}
