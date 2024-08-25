### Zookeeper

#### 为什么采用 Zookeeper 做服务发现而不是 etcd 等
- 该 mq 仿照 kafka,因为 kafka 以前使用 Zookeeper，有着更强大的生态系统，所以本项目也采用 Zookeeper 。但是 kafka 后来自己使用 raft 协议内置了一个，就移除 Zookeeper了
- Zookeeper 实现了 C（一致性）、P（分区容错性），没有 A（高可用性），Zookeeper从顺序一致性（来自任意特定客户端的更新都会按其发送顺序被提交保持一致）、原子性（更新要么成功要么失败）、持久性等方面来保证数据的一致性
- etcd 基于 raft 共识算法，提供强一致性、容错性和分区容错性，也可以选择他来存储元数据

#### SDK
采用 kitex 支持的 zookeeper 的接口，但该 SDK 并不完整，需要补充，且该 SDK 也是对 go-zookeeper/zk 的封装

#### Broker
当 Broker 启动时，需要连接到 Zookeeper,（做一个负载均衡，通过检测各个 broker 的状况来负载），在 zookeeper 上的 Broker 下创建临时节点（退出即销毁）。

启动时会恢复该节点下的信息，broker 从 zkserver 拿到已存在的自己名下的资源并创建各个 topic、partition 和 subscription

##### producer push message
当有生产者生产信息时，生产者会先连接zookeeper,查询该信息将交给谁来接收（负载均衡算法）；当有生产者生产信息时，向 zkserver 查询该向谁（broker）发送，zkserver 将发送信息到该 broker,broker检查 topic 和 partition 是否存在，不存在就创建并且启动该 partition 接收该功能（设置该 partition的接收队列和文件）

##### consumer sub and startToGet
当有消费者消费信息时，向 zkserver 查询该向谁（broker）发送请求，zkserver 判断是否订阅了该 topic/partition ，若没有则返回，若有，zkserver 向 broker 发送信息，若该 broker 没有该订阅则创建一个，并根据 file_name 创建 config，并负载均衡，发送信息；

当消费者消费信息时：连接到 Zookeeper（Broker），查询该节点是由那个 Broker 负责，重新连接该 Broker，进行消费；
- 对于 PTP 的情况，消费者将获得对该 Topic 的消费信息，和对各个节点的消费情况，连接到 Brokers 后将它们负责的 Partitions 的情况发送过去，并开始消费。
- 对于 PSB 的情况，消费者获取该 Topic——Partition 所在 Broker 节点后，将自己给出想要消费的位置进行消费。

当 zkserver 上的负载均衡发生改变时，需要将停止一些 broker 上的接受 producer 的信息的服务，转移到其他地方去。zkserver 需要发送到该 broker，停止信息，并将文件名进行修改，若有 consumer 在消费这个文件，则修改 config 中的文件名等，发送到新 broker 去接收信息。

当 borker 中一个 file 消费完后，将发送结束信息到 consumer，consumer 将再次请求 zkserver 去获取下一个 broker 的位置。 

#### 更平滑的分片迁移
当分片发生迁移时会导致磁盘 IO 和网络 IO 暴增（文件迁移），而导致分片发生迁移的原因是由于负载均衡导致的，而负载均衡又是由于加入了新的 Broker 集群或者 Broker 的性能问题；而暴增的磁盘和网络 IO 会导致短时间内达不到减轻 Broker 压力和提高性能的目的，所以 zhuMQ 不进行文件的迁移，当分片发生迁移时我们将 producer 生产的消息放在新的 Broker 位置，消费者消费完原位置的信息后再转移到新位置进行消费。