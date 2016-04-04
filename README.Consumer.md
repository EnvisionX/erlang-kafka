# Asynchronous consumer

Connects to the Kafka cluster, consumes configured partition
of a topic and send each received {Key, Value} pair to the
target Erlang process in the format:

```erlang
{kafka_consumer,
 ConsumerPID :: pid(),
 msg,
 TopicName :: kafka:topic_name(),
 PartitionID :: kafka:partition_id(),
 Offset :: kafka:offset(),
 Key :: kafka:kbytes(),
 Value :: kafka:kbytes()
}
```

## Features:

* metadata discovery - consumer will automatically choose
   Kafka node to consume from;
* auto reconnect - consumer will re-discover and re-connect
   on Kafka cluster configuration change;
* offset autocommit mode.

## Offsets and autocommit mode

Important note about message offsets. You can configure the
process to consume from the eldest available message, from
the head (latest available) of the partition, from current
saved offset for the GroupID or from absolute offset. The
messages will be consumed in order despite network failure
and reconnect attempts inside the process. But if the process
started without 'autocommit' option, it is up to you to save
consumed offsets back to the Kafka cluster.
Worth to say if you use 'autocommit' option, offsets will
be marked as 'consumed' when they received from Kafka broker,
but not when they really processed.

## Usage example

```erlang
Nodes =
    [{"10.0.0.1", 9092},
     {"10.0.0.2", 9092}],
{ok, ConsumerPID} = kafka_consumer:start_link(
    Nodes, TopicName = "my-topic", PartitionID = 0,
    _Options = [{offset, first},
                {group_id, "my-consumer-id"},
                {max_bytes, 64 * 1024}]),
receive
    {kafka_consumer, ConsumerPID, msg, TopicName, PartitionID,
     Offset, Key, Value} ->
        ok = io:format(
            "received msg at offset ~w from topic ~s, partition ~w:"
            " key=~9999p, value=~9999p~n",
            [Offset, TopicName, PartitionID, Key, Value])
end,
...
```
