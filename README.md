# Apache Kafka 0.8.x client written on Erlang

## Summary

This is a very early implementation and provides only
some of the basic features of the Kafka protocol:

- metadata requests;
- synchronous produce requests;
- asynchronous produce requests.

## License

BSD two-clause license.

## Examples

Start Kafka client as linked process:

```erlang
Brokers = [{"10.0.0.1", 9092},
           {{10,0,0,2}, 9092},
           {"kafka3.some.net", 9092}],
Options = [],
{ok, KafkaClientPid} = kafka:start_link(KafkaBrokers, KafkaOptions),
...
```

Start Kafka client as named linked process:

```erlang
...
{ok, _} = kafka:start_link({local, kafka}, KafkaBrokers, KafkaOptions),
...
```

or even:

```erlang
...
{ok, _} = kafka:start_link({global, kafka}, KafkaBrokers, KafkaOptions),
...
```

To start Kafka client as part of the supervision tree, extend
your supervisor spec with something like:

```erlang
init(_Args) ->
    {ok, {
       {one_for_one, 5, 1},
       [
        ...
        %% Apache Kafka client
        {kafka_client_worker_id,
         {kafka, start_link,
           [{local, kafka}, KafkaBrokers, KafkaOptions]},
            permanent, 100, worker, dynamic},
        ...
       ]}}.
```

Once started, the client can be referenced by PID or by registered name.

To produce some new data into a Kafka topic, do:

```erlang
    ...
    ok = kafka:produce_async(
      _KafkaClientRegisteredNameOrPid = kafka,
      _BrokerId = auto, %% autodetect leader for the topic
      [{_TopicName = "my-topic",
        [{_PartitionId = 0,
          _MessageSet =
              %% Here we send only one message in a set
              [{_Offset = 0, %% auto assign message offset
                {message,
                 _MagicByte = 0,
                 _Compression = 0, %% do not use a compression
                 _Key = null, %% this is not a keyed message
                 _Value = "Message Payload String"}}
              ]}]}]),
    ...
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

## TODO:

Request types:

* fetch request;
* offset request;
* consumer metadata request;
* offset commit request;
* offset fetch request.

Other features:

* message buffering;
* make use of round-robin filling the topic partitions during producing.

Miscellaneous tasks:

* add some code examples.
