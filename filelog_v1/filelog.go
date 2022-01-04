package filelog_v1

import (
	"errors"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 结构体1: 数据-读取内容的结构体
type UDataSend struct {
	Date          string // 日期:2021-12-12
	Time          int32  // 时间戳(秒级)
	Id            int64  // 流水号，从0开始
	DataFileIndex int16  // 数据文件序号
	DataOffset    int64  // 相对存储文件偏移量
	DataType      int16  // 数据类型
	DataLength    int32  // 内容长度
	Data          []byte // 内容
}

// 结构体2: 数据-索引存储结构体
type UDiskIndex struct {
	DataFileIndex int16 // 0:2   数据文件序号
	DataOffset    int64 // 2:10  存储文件偏移量
	DataType      int16 // 10:12 数据类型
}

// 结构体2: 数据-内容数据存储结构体
type UDiskData struct {
	DataStart  int16  // 0:2  数据文件定位
	Time       int32  // 2:6  时间戳(秒级)
	DataLength int32  // 6:10 内容长度
	Data       []byte //
}

// 结构体4： 类-日志操作结构体
type CFileLog struct {
	AutoId        int64 // 当前流水id，从0开始
	DataFileIndex int16 // 当前数据文件序号
	DataOffset    int64 // 当前数据文件偏移量

	indexFp *os.File           // 索引文件句柄
	dataFps map[int16]*os.File // 日志数据文件句柄数组，键值为数据文件编号

	date        string // 日期:2021-12-12
	folderLog   string // 日志文件所在目录
	rowsPerFile int64  // 单个数据文件最大行数

	DataStart int16        // 数据文件定位
	lockAdd   sync.RWMutex // Add函数锁
}

// 函数1：获取日志操作实例
func New(Folder string, Date string) *CFileLog {
	fileLog := &CFileLog{
		AutoId:        -1,
		DataFileIndex: -1,
		DataOffset:    0,

		indexFp: nil,
		dataFps: map[int16]*os.File{},

		date:        Date,
		folderLog:   Folder,
		rowsPerFile: 1000000,

		DataStart: 19334,
	}
	fileLog.init()
	return fileLog
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
	return Me.AutoId, nil
}

// 函数4：新增一条日志
func (Me *CFileLog) Add(Time int32, DataType int16, Data []byte) (int64, error) {
	Me.lockAdd.Lock()
	defer Me.lockAdd.Unlock()

	if Me.AutoId == -2 {
		return -1, errors.New("实例已关闭")
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
	KeyDataLength := len(Data)

	// 写数据
	if _, err := Me.dataFps[KeyDataFileIndex].Seek(Me.DataOffset, 0); err != nil {
		log.Panic("写数据数据seek失败：" + err.Error())
		return -1, err
	} else {
		b := utilsEncodeUDiskData(&UDiskData{
			DataStart:  Me.DataStart,
			Time:       Time,
			DataLength: int32(KeyDataLength),
			Data:       Data,
		})
		if _, err := Me.dataFps[KeyDataFileIndex].Write(b); err != nil {
			return -1, err
		}
	}

	// 写入索引
	if _, err := Me.indexFp.Seek(Me.AutoId*12, 0); err != nil {
		log.Panic("写数据索引seek失败：" + err.Error())
		return -1, err
	} else {
		b := utilsEncodeUDiskIndex(&UDiskIndex{
			DataFileIndex: KeyDataFileIndex, // 索引文件序号
			DataOffset:    Me.DataOffset,    // 存储文件偏移量
			DataType:      DataType,         // 数据内容类型
		})
		if _, err := Me.indexFp.Write(b); err != nil {
			return -1, err
		}
	}
	// fmt.Println(KeyDataFileIndex, Me.DataOffset, DataType, int32(KeyDataLen))

	Me.AutoId++
	// 数据文件切换时，需要重置指针偏移量
	if Me.AutoId%Me.rowsPerFile == 0 {
		Me.DataOffset = 0
	} else {
		Me.DataOffset += int64(KeyDataLength) + 10
	}

	return Me.AutoId, nil
}

// 函数5：读取一条数据
func (Me *CFileLog) GetOne(Id int64) (*UDataSend, error) {
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
	var KeyUDiskIndex *UDiskIndex
	if _, err := Me.indexFp.Seek(Id*12, 0); err != nil {
		log.Panic("取数据索引seek失败：" + err.Error())
		return nil, errors.New("索引文件指针移位失败+" + err.Error())
	} else {
		buff, err := Me.fileReadLength(Me.indexFp, 12)
		if err != nil {
			if err.Error() == "EOF" {
				return nil, nil
			} else {
				return nil, errors.New("索引文件内容读取失败+" + err.Error())
			}
		}
		KeyUDiskIndex = utilsDecodeUDiskIndex(buff)
	}

	// 内容文件句柄
	KeyUDiskData := &UDiskData{}
	if true {
		if _, ok := Me.dataFps[KeyUDiskIndex.DataFileIndex]; !ok {
			if err := Me.initFpData(KeyUDiskIndex.DataFileIndex); err != nil {
				return nil, errors.New("内容文件初始化获取失败+" + err.Error())
			}
		}
		// 读取头信息
		var Length int32
		var Buff []byte
		if _, err := Me.dataFps[KeyUDiskIndex.DataFileIndex].Seek(KeyUDiskIndex.DataOffset, 0); err != nil {
			log.Panic("取数据内容seek失败：" + err.Error())
			return nil, errors.New("内容文件指针移位失败+" + err.Error())
		} else {
			Buff, err = Me.fileReadLength(Me.dataFps[KeyUDiskIndex.DataFileIndex], 10)
			if err != nil {
				return nil, errors.New("内容文件内容读取失败+" + err.Error())
			}
			if DataStart := utilsBytes2Int16(Buff[:2]); DataStart != Me.DataStart {
				log.Panic("读取内容数据校验码失败")
			}
			Length = utilsBytes2Int32(Buff[6:10])
		}

		// 读取内容
		if buff2, err := Me.fileReadLength(Me.dataFps[KeyUDiskIndex.DataFileIndex], int(Length)); err != nil {
			return nil, errors.New("内容文件内容读取失败+" + err.Error())
		} else {
			KeyUDiskData = utilsDecodeUDiskData(append(Buff, buff2...))
		}

	}
	return &UDataSend{
		Date:          Me.date,
		Time:          KeyUDiskData.Time,
		Id:            Id,
		DataFileIndex: KeyUDiskIndex.DataFileIndex,
		DataOffset:    KeyUDiskIndex.DataOffset,
		DataType:      KeyUDiskIndex.DataType,
		DataLength:    KeyUDiskData.DataLength,
		Data:          KeyUDiskData.Data,
	}, nil
}

// 设置同步完成标记
func (Me *CFileLog) SetFinish() {
	// 判断日期目录是否存在
	folderDate, err := Me.getLogDateFolder()
	if err != nil {
		log.Panic(err)
	}

	// 判断finish文件
	f := folderDate + "/finish"
	if _, err := os.Stat(f); os.IsNotExist(err) {
		fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0644) // 打开文件
		if err != nil {
			log.Panic(err)
		}
		_ = fp.Close()
	}
}

// 读取同步完成标记
func (Me *CFileLog) GetFinish(asFinishFlag ...bool) bool {
	// 判断日期目录是否存在
	folderDate, err := Me.getLogDateFolder()
	if err != nil {
		log.Panic(err)
	}

	// 判断finish文件
	f := folderDate + "/finish"
	if _, err := os.Stat(f); os.IsNotExist(err) {
		// 判断是否close状态,如果参数为true，则只判断finish文件状态不管日期
		if len(asFinishFlag) > 0 && asFinishFlag[0] {
			return false
		} else {
			if Me.date < time.Now().Format("2006-01-02") {
				return true
			} else {
				return false
			}
		}
	} else {
		return true
	}
}

// 初始化
func (Me *CFileLog) init() {
	if err := Me.initFpIndex(); err != nil {
		log.Panic(err)
	}
	fi, _ := Me.indexFp.Stat()
	Me.AutoId = fi.Size() / 12
	Me.DataFileIndex = int16(Me.AutoId / Me.rowsPerFile)

	// 初始化内容文件偏移量
	if Me.AutoId%Me.rowsPerFile == 0 {
		Me.DataOffset = 0
	} else {
		if D, err := Me.GetOne(Me.AutoId - 1); err != nil {
			log.Panic("初始化读取当前AutoId数据失败：" + err.Error())
		} else {
			Me.DataOffset = D.DataOffset + int64(D.DataLength) + 10
		}
	}
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

// 辅助函数1：初始化内容文件句柄
func (Me *CFileLog) initFpData(index int16) error {
	// 判断日期目录是否存在,没有将会创建
	folderDate, err := Me.getLogDateFolder()
	if err != nil {
		return err
	}

	// 打开内容文件
	f := folderDate + "/data" + strings.Repeat("0", 6-len(strconv.Itoa(int(index)))) + strconv.Itoa(int(index))
	fp, err := os.OpenFile(f, os.O_RDWR|os.O_CREATE, 0644) // 打开文件
	if err != nil {
		return err
	}
	Me.dataFps[index] = fp

	return nil
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
