### 消息队列应用场景
- 流量削峰：电商网站大量用户同时下单，导致订单压力剧增，如果有消息队列，订单请求会先被发送到消息队列，然后订单系统再以相对稳定速度处理请求
- 解耦：有一个用户注册模块和一个邮件通知模块。每当用户注册成功时，需要发送一封欢迎邮件。如果这两个模块紧耦合，那么用户注册模块需要直接调用邮件模块的接口，导致两个模块之间的依赖性增加。通过引入消息队列，用户注册模块可以将“用户注册成功”的消息发送到消息队列，邮件通知模块只需要订阅这个消息队列即可接收到消息并发送邮件，这样两个模块之间不再直接依赖，达到了系统的解耦。
- 异步：用户下单时，系统需要同时处理订单的生成和库存的更新，直接同步处理可能导致整个过程耗时很长，订单量大出现性能瓶颈。如果引入消息队列，订单系统可以先将“订单已生成”的消息发送到消息队列，然后立即返回给用户订单已生成的确认。库存更新系统只需从消息队列中订阅该消息，并异步处理库存的更新操作。减少用户等待时间
- 日志分析：公司需要实时收集和分析用户行为日志，比如点击、浏览、购买等。直接将这些日志发送到数据库或分析系统会给系统带来很大的压力。通过使用消息队列，可以将这些日志事件先发送到消息队列，然后由日志分析系统从消息队列中获取并处理这些日志数据。这样不仅提高了系统的扩展性，还能保证日志处理的实时性。

### 支持的功能
- 支持多种消息模式
    - PTP
    - PSB
- 支持多种消息获取方式
  - 服务器主动将消息发给 consumer：获取消息及时，但是无法确定 consumer 端的接收速度，可能会把 consumer 的缓冲区打满，导致消息丢失
  - consumer 主动向服务器拉取消息：不需要估计 consumer 的接收速度，但是消息获取不及时，当消息比较少的情况下，需要 consumer 不断轮询服务器是否有信息
- 支持分布式（多副本）（待定）
    - 使用 raft 协议支持分布式
    - fetch 机制
- 使用 ack 确认应答机制
    - ack 机制是用来选择三种模式，三种模式对数据一致性的保证是不一样的，有强一致和弱一致，可通过需要选择，如果需要响应时间短就采用 0 或 1，如果需要可用性强，一致性强就选 -1（三种都支持，kafka有这个机制）
      - ack = -1：当消息同步到大多数节点（写入大多数节点磁盘）才返回成功
      - ack = 0 ：当消息同步到leader节点（写入leader磁盘）上就返回成功
      - ack = 1 ：当消息被leader接收（收到消息就返回）就返回
      - 问题：如果不是强一致，需要想办法将0，1 模式未同步到其他副本节点的消息同步过去（我采用的是 fetch 机制，让副本节点做为 consumer 取 pull leader 上的信息，同步到本地磁盘）
- 持久化磁盘
    - 写入本地磁盘，可读取历史消息，用于日志分析
    - 顺序读写，加快磁盘 IO
    - 稀疏索引加速消息索引查找
- 分片
    - 将消息分为多个 topic
    - 每个 topic 分为多个 partition
    - 保证每个 partition 的消息是有序的，不保证 topic 有序
    - 每个 partition 存放一个文件（文件太大就需要更换文件写）
- 存储元数据
    - 使用 Zookeeper（具体见Zookeeper.md）
- 负载均衡
    - 通过负载均衡算法平均分担集群压力，将高负载节点的分片转移到低负载的节点
- RPC
    - 采用字节跳动研发的 kitex（具体详见 kitex.md）
