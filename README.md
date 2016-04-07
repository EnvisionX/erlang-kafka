# Apache Kafka 0.8+ protocol for Erlang

## Summary

This is a very early implementation and provides only
some of the basic features of the Kafka protocol:

* synchronous and asynchronous requests;
* produce - posting data to the Kafka;
* fetch - consuming data from the Kafka;
* offsets - lookup available offsets for particular topic and partition;
* metadata - lookup available Kafka nodes and topics;
* offset commit - save consumer current position;
* offset fetch - retrieve consumer position from Kafka.

The goal of the project is to provide comprehensive Erlang bindings to
the Apache Kafka protocol as is, without any client-side extensions. So,
you should keep
[Apache Kafka protocol description]
(https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
page open when you use the library. As advantage of such approach:

* the library can be easily extended with support of new request/response types
without any changes in the API;
* the code required to add support of a new request/response is extremely small.
For example, here is the code from kafka_api module which add support for
OffsetCommit request/response API:

```erlang
...
req_payload(?OffsetCommit, 0 = _ApiVersion) ->
    ?Struct(
       [string, %% group_id
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions
                  ?Struct(
                     [int32, %% partition_id
                      int64, %% offset
                      string %% metadata
                     ]))
              ]))
       ]);
...
resp_payload(?OffsetCommit, 0 = _ApiVersion) ->
    ?ArrayOf( %% responses
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partition_responses
              ?Struct(
                 [int32, %% partition_id
                  int16 %% error_code
                 ]))
          ]));
...
```

So, if you looking for high level client library with bells and whistles, you
should better use:

* [ekaf](https://github.com/helpshift/ekaf) - Kafka producer library with buffering,
 batch send and other convenient features.
* [brod](https://github.com/klarna/brod) - Kafka client which provide nice
 Erlang MFAs to make several commonly used Kafka requests.

But if you want to build you own logic based on bare Apache Kafka protocol - you
are on the right way.

## License

BSD two-clause license.

## Examples

### Start Kafka client as linked process:

```erlang
Host = "10.0.0.1",
Port = 9092,
{ok, Socket} = kafka_socket:start_link(Host, Port, []),
...
```

The example code snippet above will crash on connection error. There is a
special option for kafka_socket:start_link/3 function which make the
process to behave more stable in case of network failures:

```erlang
{ok, Socket} = kafka_socket:start_link("10.0.0.1", 9092, [reconnect]),
...
```

In this case the process will not terminate until kafka_socket:close/1
will be called and will try to continuously reconnect to the broker if
first connection attempt was failed or when already established connection
closed for some reason.

### Request metadata from the broker:

```erlang
case kafka_socket:sync(Socket,
                       _ApiKey = 3,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       _MetadataRequest = [],
                       _SockReadTimeout = infinity) of
    {ok, {Brokers, Topics} = _Metadata} ->
        [... || {BrokerID, Host, Port} <- Brokers],
        [... || {TopicErrorCode, TopicName, Partitions} <- Topics,
                {PartitionErrorCode, PartitionID, LeaderBrokerID,
                 ReplicaBrokerIDs, InSyncReplicaBrokerIDs} <- Partitions],
        ...;
    {error, Reason} ->
        ...
end,
```

or use an existing bootstrap wrapper which will connect, make a
metadata request and disconnect:

```erlang
case kafka:metadata(_Host = "10.0.0.1", _Port = 9092) of
    {ok, Brokers, Topics} ->
        [... || {BrokerID, Host, Port} <- Brokers],
        [... || {TopicErrorCode, TopicName, Partitions} <- Topics,
                {PartitionErrorCode, PartitionID, LeaderBrokerID,
                 ReplicaBrokerIDs, InSyncReplicaBrokerIDs} <- Partitions],
        ...;
    {error, Reason} ->
        ...
end,
```

### Make a produce request:

```erlang
Topics =
    [{_TopicName = "my-topic",
      _Partitions =
          [{_PartitionID = 0,
            _MessageSet =
                [{_AutoOffset = -1,
                  _Message =
                      {_MagicByte = 1,
                       _Attributes = 0,
                       _Key = <<"k2">>,
                       _Value = <<"v2">>}}]}]}],
ProduceRequest = {_AckAfterLocalWrite = 1, _TimeoutMillis = 1000, Topics},
case kafka_socket:sync(Socket,
                       _ApiKey = 3,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       ProduceRequest,
                       _SockReadTimeout = infinity) of
    {ok, [{_TopicName, Partitions}]} ->
        [... || {PartitionID, ErrorCode, BaseOffset} <- Partitions],
        ...;
    {error, Reason} ->
        ...
end,
```

### Make an asynchronous produce request:

```erlang
Topics =
    [{_TopicName = "my-topic",
      _Partitions =
          [{_PartitionID = 0,
            _MessageSet =
                [{_AutoOffset = -1,
                  _Message =
                      {_MagicByte = 1,
                       _Attributes = 0,
                       _Key = <<"k2">>,
                       _Value = <<"v2">>}}]}]}],
ProduceRequest = {_NoAcknowledge = 0, _TimeoutMillis = 1000, Topics},
case kafka_socket:async(Socket,
                        _ApiKey = 3,
                        _ApiVersion = 0,
                        _ClientID = "my-client",
                        ProduceRequest) of
    ok ->
        ...;
    {error, Reason} ->
        ...
end,
```

### Make a fetch (consume) request:

```erlang
Topics =
    [{_TopicName = "my-topic",
      _Partitions = [{_PartitionID = 0,
                      _FetchOffset = 0,
                      _MaxBytes = 1024}]}],
FetchRequest = {_ReplicaID = -1, _MaxWaitTimeMillis = 1000, _MinBytes = 1, Topics},
case kafka_socket:sync(Socket,
                       _ApiKey = 1,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       FetchRequest,
                       _SockReadTimeout = infinity) of
    {ok, TopicReplies} ->
        [... || {TopicName, Partitions} <- TopicReplies,
                {PartitionID, ErrorCode, HighWatermark, MessageSet} <- Partitions,
                {Offset, {MagicByte, Attrs, Key, Value}} <- MessageSet],
        ...;
    {error, Reason} ->
        ...
end,
```

### Make an offsets request:

```erlang
OffsetsRequest =
    {_ReplicaID = -1,
     _Topics = [{_TopicName = "my-topic",
                 _Partitions = [{_PartitionID = 0,
                                 _Timestamp = -1, %% special value means 'latest offset'
                                 _MaxNumOffsets = 1}]}]},
case kafka_socket:sync(Socket,
                       _ApiKey = 2,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       OffsetsRequest,
                       _SockReadTimeout = infinity) of
    {ok, TopicReplies} ->
        [... || {TopicName, Partitions} <- TopicReplies,
                {PartitionID, ErrorCode, Offsets} <- Partitions,
                Offset <- Offsets],
        ...;
    {error, Reason} ->
        ...
end,
```

### Make an offset commit request:

```erlang
OffsetCommitRequest =
    {_GroupID = "my-consumer-id",
     _Topics = [{_TopicName = "my-topic",
                 _Partitions = [{_PartitionID = 0,
                                 _Offset = 1234,
                                 _Metadata = "any data"}]}]},
case kafka_socket:sync(Socket,
                       _ApiKey = 8,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       OffsetCommitRequest,
                       _SockReadTimeout = infinity) of
    {ok, TopicReplies} ->
        [... || {TopicName, Partitions} <- TopicReplies,
                {PartitionID, ErrorCode} <- Partitions],
        ...;
    {error, Reason} ->
        ...
end,
```

### Make an offset fetch request:

```erlang
OffsetFetchRequest =
    {_GroupID = "my-consumer-id",
     _Topics = [{_TopicName = "my-topic",
                 _Partitions = [0, 1, 2]}]},
case kafka_socket:sync(Socket,
                       _ApiKey = 9,
                       _ApiVersion = 0,
                       _ClientID = "my-client",
                       OffsetFetchRequest,
                       _SockReadTimeout = infinity) of
    {ok, TopicReplies} ->
        [... || {TopicName, Partitions} <- TopicReplies,
                {PartitionID, Offset, Metadata, ErrorCode} <- Partitions],
        ...;
    {error, Reason} ->
        ...
end,
```

## Build dependencies

* GNU Make;
* Erlang OTP;
* erlang-dev, erlang-tools (only when they are not provided with the Erlang OTP, e.g. in Debian);
* erlang-edoc (optional, needed to generate HTML docs);
* erlang-dialyzer (optional, needed to run the Dialyzer).

## Runtime dependencies

* Erlang OTP.

## Build

```make compile```

## Generate an HTML documentation for the code

```make html```

## Dialyze the code

```make dialyze```

## TODO

* implement message set compression;
* add codec specs for most of Apache Kafka request/response pairs.
