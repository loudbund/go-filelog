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
	Date          string // 日期:2021-12-12
	Id            int64  // 流水号，从0开始
	DataFileIndex int16  // 数据文件序号
	DataOffset    int64  // 相对存储文件偏移量
	DataType      int16  // 数据类型
	DataLength    int32  // 内容长度
	Data          []byte // 内容
}

// 结构体2： 日志操作结构体
type CFileLog struct {
	AutoId        int64 // 当前流水id，从0开始
	DataFileIndex int16 // 当前数据文件序号
	DataOffset    int64 // 当前数据文件偏移量

	indexFp *os.File           // 索引文件句柄
	dataFps map[int16]*os.File // 日志数据文件句柄数组，键值为数据文件编号

	date        string // 日期:2021-12-12
	folderLog   string // 日志文件所在目录
	rowsPerFile int64  // 单个数据文件最大行数
}

// 函数1：获取日志操作实例
func New(Folder string, Date string) *CFileLog {
	return &CFileLog{
		AutoId:        -1,
		DataFileIndex: -1,
		DataOffset:    0,

		indexFp: nil,
		dataFps: map[int16]*os.File{},

		date:        Date,
		folderLog:   Folder,
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
		fmt.Println("AutoId:", Me.AutoId)
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
	KeyDataFileIndex := int16(Me.AutoId / Me.rowsPerFile)
	if _, ok := Me.dataFps[KeyDataFileIndex]; !ok {
		if err := Me.initFpData(KeyDataFileIndex); err != nil {
			return -1, err
		}
	}

	// 内容长度
	KeyDataLen := len(Data)

	// 写入索引
	if _, err = Me.indexFp.Seek(Me.AutoId*16, 0); err != nil {
		return -1, err
	} else {
		b := bytes.NewBuffer([]byte{})
		b.Write(utilsInt16ToBytes(KeyDataFileIndex))  // 索引文件序号
		b.Write(utilsInt64ToBytes(Me.DataOffset))     // 存储文件偏移量
		b.Write(utilsInt16ToBytes(DataType))          // 数据内容类型
		b.Write(utilsInt32ToBytes(int32(KeyDataLen))) // 数据长度
		if _, err := Me.indexFp.Write(b.Bytes()); err != nil {
			return -1, err
		}
	}
	// fmt.Println(KeyDataFileIndex, Me.DataOffset, DataType, int32(KeyDataLen))

	// 写数据
	if _, err = Me.dataFps[KeyDataFileIndex].Seek(Me.DataOffset, 0); err != nil {
		return -1, err
	} else {
		if _, err := Me.dataFps[KeyDataFileIndex].Write(Data); err != nil {
			return -1, err
		}
		Me.DataOffset += int64(KeyDataLen)
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
	var KeyDataFileIndex int16
	var KeyDataType int16
	var KeyDataLength int32
	var KeyOffset int64
	if _, err := Me.indexFp.Seek(Id*16, 0); err != nil {
		return nil, errors.New("索引文件指针移位失败+" + err.Error())
	} else {
		buff, err := Me.fileReadLength(Me.indexFp, 16)
		if err != nil {
			if err.Error() == "EOF" {
				return nil, nil
			} else {
				return nil, errors.New("索引文件内容读取失败+" + err.Error())
			}
		}
		KeyDataFileIndex = int16(binary.BigEndian.Uint16(buff[:2]))
		KeyOffset = int64(binary.BigEndian.Uint64(buff[2:10]))
		KeyDataType = int16(binary.BigEndian.Uint16(buff[10:12]))
		KeyDataLength = int32(binary.BigEndian.Uint32(buff[12:16]))
	}

	// 内容文件句柄
	var KeyDataBuff []byte
	if true {
		fN := int16(Id / Me.rowsPerFile)
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
		Date:          Me.date,
		Id:            Id,
		DataFileIndex: KeyDataFileIndex,
		DataOffset:    KeyOffset,
		DataType:      KeyDataType,
		DataLength:    KeyDataLength,
		Data:          KeyDataBuff,
	}, nil
}

// 辅助函数1：初始化内容文件句柄
func (Me *CFileLog) initFpData(index int16) error {
	// 判断日期目录是否存在,没有将会创建
	folderDate, err := Me.getLogDateFolder()
	if err != nil {
		return err
	}

	// 打开内容文件
	f := folderDate + "/data" + strings.Repeat("0", 6-len(strconv.Itoa(int(index)))) + strconv.Itoa(int(index))
	fp, err := os.OpenFile(f, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644) // 打开文件
	if err != nil {
		return err
	}
	Me.dataFps[index] = fp

	return nil
}

// 辅助函数2：初始化自增id
func (Me *CFileLog) initAutoId() error {
	if err := Me.initFpIndex(); err != nil {
		return err
	}
	fi, _ := Me.indexFp.Stat()
	Me.AutoId = fi.Size() / 16
	Me.DataFileIndex = int16(Me.AutoId / Me.rowsPerFile)

	// 初始化内容文件偏移量
	if Me.AutoId == 0 {
		Me.DataOffset = 0
	} else {
		if D, err := Me.GetOne(Me.AutoId - 1); err != nil {
			fmt.Println("初始化读取当前AutoId数据失败", err)
		} else {
			Me.DataOffset = D.DataOffset
		}
	}

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
