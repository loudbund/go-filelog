# go-filelog
go-filelog 是用来将大量日志写入日志文件和读取日志文件的封装，具有以下特点
1. 日期按天分文件夹存储，编译整体拷贝和归档删除
2. 一天的日志文件存储分为索引文件和数据文件，索引文件只有1个文件，数据文件按设置条数切割


## 安装
go get github.com/loudbund/go-filelog

## 引入
```golang
import "github.com/loudbund/go-filelog/filelog_v1"
```

## 使用

|序号|名称|函数|
|:---:|---|---|
|1|实例化一个日期日志句柄|filelog_v1.New(Folder string, Date string) *CFileLog |
|2|关闭释放当前日期日志句柄|handle.Close() |
|3|写入一条日志|handle.Add(Time int32, DataType int16, Data []byte) (int64, error)|
|4|读出一条日志|handle.GetOne(Id int64) (*UDataSend, error)|
|5|设置日期日志已结束|handle.SetFinish()|
|6|读取日期日志已结束|handle.GetFinish(asFinishFlag ...bool) bool|
|7|获取下一日志自增序号|handle.GetAutoId() (int64, error) |


1、 获取操作句柄，一个日期一个句柄
```golang	
handle := filelog_v1.New("/tmp/test-filelog/", "2021-12-28")
```

2、  写入日志
```golang
handle := filelog_v1.New("/tmp/test-filelog/", "2021-12-28")
_, _ := handle.Add(0, []byte("hello world"))
```

3、 读取日志
```golang
handle := filelog_v1.New("/tmp/test-filelog/", "2021-12-28")
_, _ := handle.GetOne(0)
```

## example
详见example.go