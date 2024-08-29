## zhuMQ
zhuMQ 是一款运行于 Linux 操作系统，由 Golang 语言开发的分布式面向消息的中间件,能够实时存储和分发数据。

- 支持多种订阅模式（点对点和发布订阅），用户可根据不同应用场景选择消息消费模式
- 引入 Zookeeper 存储元数据，保证数据一致性和高可用
- 多种同步副本方式，根据 ack 机制选择同步方式
- 支持分布式，可以提高可扩展性和容错性
- 采用 SSTable 标记索引位置，加速消息索引查询
- 顺序读写磁盘，提高数据读写速率

### 使用说明

#### 安装
zhuMQ 使用 cloudwego 的 kitex 作为 rpc 框架，所以需要先安装 kitex
- 安装 Go：https://golang.org/
- 安装 kitex：go install github.com/cloudwego/kitex/tool/cmd/kitex@latest
- 安装 thriftgo：go install github.com/cloudwego/thriftgo@latest

可参考[kitex](https://www.cloudwego.io/zh/docs/kitex/getting-started/)

安装 zhuMQ：
- 拉取 zhuMQ
```
git clone git@github.com:zhuheqqq/zhuMQ.git
```
- 使用 kitex 生成 RPC 代码
```
cd zhuMQ
kitex -module zhuMQ operations.thrift 
kitex -module zhuMQ raftoperations.thrift 
```

接下来就可以开始在你的代码中使用 zhuMQ。