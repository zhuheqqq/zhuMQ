namespace go api

struct PushRequest {
    1: string producer
    2: string topic
    3: string key
    4: binary message
    5: i64    Endindex
    6: i64    StartIndex
    7: i8     Size
}

struct PushResponse {
    1: bool     ret
    2: string   err
}

struct PullRequest {
    1: string   consumer
    2: string   topic
    3: string   key
    4: i64    offset
    5: i8     size
}

struct PullResponse {
     1: binary   Msgs
     2: bool     Ret
     3: i64      Start_index
     4: i64      End_index
     5: i8       Size
     6: string   Err
}

//consmer发送自己的host和ip,使broker连接上自己
struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool     ret
}


//consumer准备开始接收信息
struct InfoGetRequest {
    1: string   cli_name
    2: string   topic_name
    3: string   part_name
    4: i64      offset
    5: i8       option
}

struct InfoGetResponse {
    1: bool  ret
}

//设置某个partition接受信息的文件和队列
struct StartGetMessageRequest {
    1:  string topic_name
    2:  string part_name
    3:  string file_name
}

struct StartGetMessageResponse {
    1: bool    ret
}

struct SubRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct SubResponse {
    1: bool ret
}

//关闭某个partition，停止接收信息，
struct CloseGetMessageRequest{
    1:  string topic_name
    2:  string part_name
    3:  string file_name
    4:  string new_name
}

struct CloseGetMessageResponse{
    1:  bool    ret
}

struct CloseAcceptRequest{
    1: string topic_name
    2: string part_name
    3: string oldfilename
    4: string newfilename
}

struct CloseAcceptResponse{
    1: bool ret
    2: i64 startindex
    3: i64 endindex
}

//zkserver
struct PrepareAcceptRequest{
    1:  string  topic_name
    2:  string  part_name
    3:  string  file_name
}

struct PrepareAcceptResponse{
    1:  bool    ret
    2:  string  err
}

struct PrepareSendRequest{
    1:  string  consumer
    2:  string  topic_name
    3:  string  part_name
    4:  string  file_name
    5:  i8      option
    6:  i64     offset
}

struct PrepareSendResponse{
    1:  bool    ret
    2:  string  err   //若已经准备好“had_done”
}

struct PrepareStateRequest{
    1:  string  TopicName
    2:  string  PartName
    3:  i8      State
    4:  binary  Brokers
}

struct PrepareStateResponse{
    1:  bool    Ret
    2:  string  Err
}

struct AddRaftPartitionRequest {
    1:  string  TopicName
    2:  string  PartName
    3:  binary  Brokers
}

struct AddRaftPartitionResponse{
    1:  bool    Ret
    2:  string  Err
}

struct CloseRaftPartitionRequest{
    1:  string  TopicName
    2:  string  PartName
}

struct CloseRaftPartitionResponse{
    1:  bool    Ret
    2:  string  Err
}

struct AddFetchPartitionRequest{
    1:  string  TopicName
    2:  string  PartName
    3:  string  LeaderBroker
    4:  string  HostPort
    5:  binary  Brokers
    6:  string  FileName
}

struct AddFetchPartitionResponse{
    1:  bool    Ret
    2:  string  Err
}

struct CloseFetchPartitionRequest{
    1:  string  TopicName
    2:  string  PartName
}

struct CloseFetchPartitionResponse{
    1:  bool    Ret
    2:  string  Err
}


service Server_Operations {
    PushResponse    push(1: PushRequest req)
    PullResponse    pull(1: PullRequest req)
    InfoResponse    ConInfo(1: InfoRequest req)
    InfoGetResponse StarttoGet(1: InfoGetRequest req)

     //zkserver used this rpc to request broker server
    PrepareAcceptResponse PrepareAccept(1: PrepareAcceptRequest req)
    PrepareSendResponse PrepareSend(1: PrepareSendRequest req)
    CloseAcceptResponse     CloseAccept(1: CloseAcceptRequest req)
    PrepareStateResponse    PrepareState(   1: PrepareStateRequest  req)

    AddRaftPartitionResponse        AddRaftPartition(   1: AddRaftPartitionRequest      req)
    CloseRaftPartitionResponse      CloseRaftPartition( 1: CloseRaftPartitionRequest    req)
    AddFetchPartitionResponse       AddFetchPartition(  1: AddFetchPartitionRequest     req)
    CloseFetchPartitionResponse     CloseFetchPartition(1: CloseFetchPartitionRequest   req)
}

struct PubRequest {
    1: string topic_name
    2: string part_name
    3: i64    start_index
    4: i64    end_index
    5: binary msg
}

struct PubResponse {
    1: bool ret
}

struct PingPongRequest {
    1: bool ping
}

struct PingPongResponse {
    1: bool pong
}


service Client_Operations {
    PubResponse pub(1: PubRequest req)
    PingPongResponse pingpong(1: PingPongRequest req)
}

struct BroInfoRequest {
    1: string broker_name
    2: string broker_host_port
}

struct BroInfoResponse {
    1: bool ret
}

struct ProGetBrokRequest {
    1:  string  topic_name
    2:  string  part_name
}

struct ProGetBrokResponse {
    1:  bool    ret
    2:  string  broker_host_port
    3:  string  Err
}

struct BroGetConfigRequest {
    1:  binary  propertyinfo
}

struct BroGetConfigResponse {
    1:  bool    ret
    2:  binary  brokerinfo
}

struct ConGetBrokRequest {
    1:  string topic_name
    2:  string part_name
    3:  i8 option
}

struct ConGetBrokResponse   {
    1:  bool    ret
    2:  i64     size
    3:  binary  broks
    4:  binary  parts
}

//consumer 请求zkserver
struct ConStartGetBrokRequest {
    1:  string  cli_name
    2:  string  topic_name
    3:  string  part_name
    4:  i8      option
    5:  i64     index
}
        //返回brokers
struct ConStartGetBrokResponse   {
    1:  bool    ret
    2:  i64     size
    3:  binary  parts
//    3:  binary  broks
}

struct CreateTopicRequest {
    1:  string  topic_name
}

struct CreateTopicResponse {
    1:  bool    ret
    2:  string  err
}

struct CreatePartRequest {
    1:  string  topic_name
    2:  string  part_name
}

struct CreatePartResponse {
    1:  bool    ret
    2:  string  err
}

struct UpdateOffsetRequest{
    1: string   topic
    2: string   part
    3: i64      offset
}

struct UpdateOffsetResponse{
    1: bool     ret
}

struct SetPartitionStateRequest{
    1: string   topic
    2: string   partition
    3: i8       option
    4: i8       dupnum
}

struct SetPartitionStateResponse{
    1: bool     ret
    2: string   err
}


service ZkServer_Operations {
   SubResponse  Sub(1:  SubRequest  req)               //consumer used
   CreateTopicResponse CreateTopic(1: CreateTopicRequest req)
   CreatePartResponse  CreatePart(1: CreatePartRequest req)
   SetPartitionStateResponse SetPartitionState(1: SetPartitionStateRequest req)
    //broker
   BroInfoResponse BroInfo(1: BroInfoRequest req)
   UpdateOffsetResponse UpdateOffset(1: UpdateOffsetRequest req)
   ProGetBrokResponse ProGetBroker(1:  ProGetBrokRequest req)//broker 发送info让zkserver连接broker
   ConStartGetBrokResponse ConStartGetBroker(1:  ConStartGetBrokRequest req)

       //broker 用于恢复缓存的，暂时不使用
   BroGetConfigResponse BroGetConfig(1:    BroGetConfigRequest req)
}