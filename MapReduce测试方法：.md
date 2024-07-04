# MapReduce测试方法：

## 环境

这个作业是在旧版本的Go下面写的，所以需要修改下GoPath还有Go111module

```bahs
go env -w GO111MODULE='auto' GOPATH='目录/6.824.bat/'
```

## 准备插件

> 每次修改mrmaster/mrworker的代码后就得重新生成

```go
go build -buildmode=plugin ../mrapps/wc.go
```

## 执行测试sh

```bash
cd 项目目录/src/main
sh test-mr.sh

##接着会输出各种测试结果
```