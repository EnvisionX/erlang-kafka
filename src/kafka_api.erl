%%% @doc
%%% Apache Kafka API specifications.
%%%
%%% The module contains request/response message specifications,
%%% used to encode/decode the messages for network transmission.
%%%
%%% For more details about specifications see kafka_codec module.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 02 Apr 2016
%%% @copyright 2016, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka_api).

%% API exports
-export([req/2, resp/2, message_set/1]).

-include("kafka.hrl").
-include("kafka_types.hrl").
-include("kafka_constants.hrl").

-define(ArrayOf(Type), [Type]).
-define(Struct(ElemTypes), {ElemTypes}).

%% ----------------------------------------------------------------------
%% API functions
%% ----------------------------------------------------------------------

%% @doc Return request message specification for encoding/decoding.
-spec req(ApiKey :: int16(), ApiVersion :: int16()) -> kafka_codec:ktype().
req(ApiKey, ApiVersion) ->
    ?Struct(
       [int16, %% api_key
        int16, %% api_version
        int32, %% corellation_id
        string, %% client_id
        req_payload(ApiKey, ApiVersion) %% request message payload
       ]).

%% @doc Return response message specification for encoding/decoding.
-spec resp(ApiKey :: int16(), ApiVersion :: int16()) -> kafka_codec:ktype().
resp(ApiKey, ApiVersion) ->
    ?Struct(
       [int32, %% corellation_id
        resp_payload(ApiKey, ApiVersion) %% response message payload
       ]).

%% @doc Return codec specification for MessageSet structure.
%% This part of the request/response messages is exported because
%% message sets can be recursively nested (when compression is applied).
%% So we allow user to play with message sets by himself.
-spec message_set(ApiVersion :: int16()) -> kafka_codec:ktype().
message_set(0) ->
    {mset,
     ?Struct(
        [int64, %% offset
         {msg,
          ?Struct(
             [int8, %% magic byte
              int8, %% attributes
              bytes, %% key
              bytes %% value
             ])}
        ])};
message_set(1) ->
    {mset,
     ?Struct(
        [int64, %% offset
         {msg,
          ?Struct(
             [int8, %% magic byte
              int8, %% attributes
              int64, %% timestamp
              bytes, %% key
              bytes %% value
             ])}
        ])}.

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Return specification for payload of request message.
-spec req_payload(ApiKey :: int16(), ApiVersion :: int16()) ->
                         kafka_codec:ktype().
req_payload(?Produce, ApiVersion) ->
    ?Struct(
       [int16, %% acks
        int32, %% timeout
        ?ArrayOf( %% topic_data
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% data
                  ?Struct(
                     [int32, %% partition
                      message_set(ApiVersion) %% record_set
                     ]))
              ]))
       ]);
req_payload(?Fetch, 0) ->
    ?Struct(
       [int32, %% replica_id
        int32, %% max_wait_time
        int32, %% min_bytes
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions
                  ?Struct(
                     [int32, %% partition
                      int64, %% fetch_offset
                      int32 %% max_bytes
                     ]))
              ]))
       ]);
req_payload(?Fetch, 1) ->
    req_payload(?Fetch, 0);
req_payload(?Fetch, 2) ->
    req_payload(?Fetch, 1);
req_payload(?Offsets, 0) ->
    ?Struct(
       [int32, %% replica_id
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions
                  ?Struct(
                     [int32, %% partition
                      int64, %% timestamp
                      int32 %% max_num_offsets
                     ]))
              ]))
       ]);
req_payload(?Offsets, 1) ->
    req_payload(?Offsets, 0);
req_payload(?Offsets, 2) ->
    req_payload(?Offsets, 1);
req_payload(?Metadata, 0) ->
    ?ArrayOf(string); %% topic_names
req_payload(?Metadata, 1) ->
    req_payload(?Metadata, 0);
req_payload(?Metadata, 2) ->
    req_payload(?Metadata, 1);
req_payload(?OffsetCommit, 0) ->
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
req_payload(?OffsetCommit, 1) ->
    ?Struct(
       [string, %% group_id
        int32, %% group_generation_id
        string, %% member_id
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions
                  ?Struct(
                     [int32, %% partition_id
                      int64, %% offset
                      int64, %% timestamp
                      string %% metadata
                     ]))
              ]))
       ]);
req_payload(?OffsetCommit, 2) ->
    ?Struct(
       [string, %% group_id
        int32, %% group_generation_id
        string, %% member_id
        int64, %% retention_time
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions
                  ?Struct(
                     [int32, %% partition_id
                      int64, %% offset
                      int64, %% timestamp
                      string %% metadata
                     ]))
              ]))
       ]);
req_payload(?OffsetFetch, 0) ->
    ?Struct(
       [string, %% group_id
        ?ArrayOf( %% topics
           ?Struct(
              [string, %% topic_name
               ?ArrayOf(int32) %% partitions
              ]))
       ]);
req_payload(?OffsetFetch, 1) ->
    req_payload(?OffsetFetch, 0);
req_payload(?OffsetFetch, 2) ->
    req_payload(?OffsetFetch, 0).

%% @doc Return specification for payload of response message.
-spec resp_payload(ApiKey :: int16(), ApiVersion :: int16()) ->
                          kafka_codec:ktype().
resp_payload(?Produce, 0) ->
    ?ArrayOf(
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partitions_responses array
              ?Struct(
                 [int32, %% partition
                  int16, %% error_code
                  int64 %% base_offset
                 ]))
          ]));
resp_payload(?Produce, 1) ->
    ?Struct(
       [?ArrayOf( %% as for v0
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partitions_responses array
                  ?Struct(
                     [int32, %% partition
                      int16, %% error_code
                      int64 %% base_offset
                     ]))
              ])),
        int32 %% throttle_time_ms
       ]);
resp_payload(?Produce, 2) ->
    resp_payload(?Produce, 1);
resp_payload(?Fetch, 0) ->
    ?ArrayOf(
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partition_responses
              ?Struct(
                 [int32, %% partition_id
                  int16, %% error_code
                  int64, %% high_watermark
                  message_set(0)
                 ]))
          ]));
resp_payload(?Fetch, 1) ->
    ?Struct(
       [int32, %% throttle_time_ms
        ?ArrayOf( %% responses
           ?Struct(
              [string, %% topic_name
               ?ArrayOf( %% partition_responses
                  ?Struct(
                     [int32, %% partition_id
                      int16, %% error_code
                      int64, %% high_watermark
                      message_set(1)
                     ]))
              ]))]);
resp_payload(?Fetch, 2) ->
    resp_payload(?Fetch, 1);
resp_payload(?Offsets, 0) ->
    ?ArrayOf( %% responses
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partition_responses
              ?Struct(
                 [int32, %% partition_id
                  int16, %% error_code
                  ?ArrayOf(int64) %% offsets
                 ]))
          ]));
resp_payload(?Offsets, 1) ->
    resp_payload(?Offsets, 0);
resp_payload(?Offsets, 2) ->
    resp_payload(?Offsets, 1);
resp_payload(?Metadata, 0) ->
    ?Struct(
       [?ArrayOf( %% brokers
           ?Struct(
              [int32, %% node_id
               string, %% host
               int32 %% port
              ])
          ),
        ?ArrayOf( %% topic_metadata
           ?Struct(
              [int16, %% topic_error_code
               string, %% topic_name
               ?ArrayOf(
                  ?Struct(
                     [int16, %% partition_error_code
                      int32, %% partition_id
                      int32, %% leader_node_id
                      ?ArrayOf(int32), %% replica node ids
                      ?ArrayOf(int32) %% in-sync-with-leader replica node ids
                     ]))
              ])
          )]);
resp_payload(?Metadata, 1) ->
    resp_payload(?Metadata, 0);
resp_payload(?Metadata, 2) ->
    resp_payload(?Metadata, 1);
resp_payload(?OffsetCommit, 0) ->
    ?ArrayOf( %% responses
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partition_responses
              ?Struct(
                 [int32, %% partition_id
                  int16 %% error_code
                 ]))
          ]));
resp_payload(?OffsetCommit, 1) ->
    resp_payload(?OffsetCommit, 0);
resp_payload(?OffsetCommit, 2) ->
    resp_payload(?OffsetCommit, 1);
resp_payload(?OffsetFetch, 0) ->
    ?ArrayOf( %% responses
       ?Struct(
          [string, %% topic_name
           ?ArrayOf( %% partition_responses
              ?Struct(
                 [int32, %% partition_id
                  int64, %% offset
                  string, %% metadata
                  int16 %% error_code
                 ]))
          ]));
resp_payload(?OffsetFetch, 1) ->
    resp_payload(?OffsetFetch, 0);
resp_payload(?OffsetFetch, 2) ->
    resp_payload(?OffsetFetch, 1).
