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