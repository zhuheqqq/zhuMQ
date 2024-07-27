namespace go api

struct PushRequest {
    1: string producer
    2: string topic
    3: string key
    4: string message
}

struct PushResponse {
    1: bool     ret
}

struct PullRequest {
    1: string   consumer
    2: string   topic
    3: string   key
}

struct PullResponse {
    1: string message
}

struct InfoRequest {
    1: string ip_port
}

struct InfoResponse {
    1: bool     ret
}

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


service Server_Operations {
    PushResponse    push(1: PushRequest req)
    PullResponse    pull(1: PullRequest req)
    InfoResponse    info(1: InfoRequest req)
    SubResponse     Sub(1:  SubRequest  req)
    InfoGetResponse StarttoGet(1: InfoGetRequest req)
}

struct PubRequest {
    1: string topic_name
    2: string part_name
    3: i64    offset
    4: binary meg
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

struct SubRequest {
    1: string consumer
    2: string topic
    3: string key
    4: i8 option
}

struct SubResponse {
    1: bool ret
}


service Client_Operations {
    PubResponse pub(1: PubRequest req)
    PingPongResponse pingpong(1: PingPongRequest req)
}