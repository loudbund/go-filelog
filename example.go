package main

import (
	"fmt"
	"github.com/loudbund/go-filelog/filelog_v1"
	"time"
)

func main() {
	write()
	scan()
	getFinish()
	setFinish()
	getFinish()
}

// 测试日志写入
// filelog_v1.New(folder,date)
// handle.Add(……)
func write() {
	defer func(T time.Time) { fmt.Println(time.Since(T)) }(time.Now())

	// 1、获取句柄，参数为
	handle := filelog_v1.New("/tmp/test-filelog/", time.Now().Format("2006-01-02"))

	// 2、调用 handle.Add 循环写入一批数据
	for i := 0; i < 11; i++ {
		_, err := handle.Add(int32(time.Now().Unix()), 31, []byte(time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			fmt.Println(err)
		}
	}
}

// handle.GetOne(index) 测试日志遍历读取
func scan() {
	// 1、获取句柄
	handle := filelog_v1.New("/tmp/test-filelog/", time.Now().Format("2006-01-02"))

	fmt.Println("index", "Date", "Time", "Id", "DataFileIndex", "DataOffset", "DataLength", "DataType", "Data", "err")

	// 2、遍历所有数据
	index := int64(0)
	for {
		// 读取一条数据
		D, err := handle.GetOne(index)
		if err != nil {
			fmt.Println(err)
			break
		}
		// 读取到结尾了
		if D == nil {
			break
		}
		fmt.Println(index, D.Date, D.Time, D.Id, D.DataFileIndex, D.DataOffset, D.DataLength, D.DataType, string(D.Data), err)
		// 读取的数据位置+1
		index++
	}
	handle.Close()
	fmt.Println(handle.GetOne(0))
}

// handle.GetFinish() 获取日期日志文件是否结束标记
func getFinish() {
	handle := filelog_v1.New("/tmp/test-filelog/", time.Now().Format("2006-01-02"))
	fmt.Println("finish:", handle.GetFinish())
}

// handle.SetFinish() 设置日期日志文件结束标记
func setFinish() {
	handle := filelog_v1.New("/tmp/test-filelog/", time.Now().Format("2006-01-02"))
	handle.SetFinish()
}
