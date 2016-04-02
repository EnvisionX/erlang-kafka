%%% @doc
%%% Erlang Client for the Apache Kafka 0.8+.
%%%
%%% Unlike most Erlang libraries, this main module contains almost
%%% nothing interesting.
%%% To manage connection to the Kafka broker and issue requests
%%% use kafka_socket module instead.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 04 Aug 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka).

%% API exports
-export(
   [metadata/2,
    crd/7
   ]).

-include("kafka.hrl").
-include("kafka_types.hrl").
-include("kafka_constants.hrl").

%% these types are defined in include/kafka_types.hrl
-export_types(
   [int8/0,
    int16/0,
    int32/0,
    int64/0,
    kstring/0,
    kbytes/0
   ]).

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Connect to broker, make a metadata request for all existing
%% topics and disconnect.
%% This function is convenient wrapper to make a bootstrapping
%% metadata request to a Kafka broker.
-spec metadata(Host :: kafka_socket:host(),
               Port :: inet:port_number()) ->
                      {ok, Brokers :: list(), Topics :: list()} |
                      {error, Reason :: any()}.
metadata(Host, Port) ->
    case crd(Host, Port, ?Metadata, _ApiVersion = 0,
             _ClientID = "erlang-kafka", _TopicsToFetch = [],
             _ReadSockTimeout = infinity) of
        {ok, {Brokers, Topics}} ->
            {ok, Brokers, Topics};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Connect to broker, make a request and disconnect.
%% This function is created mostly for debugging purposes.
%% Unlikely you want to establish new TCP connection on each subsequent
%% produce/consume/etc request in production system.
-spec crd(Host :: kafka_socket:host(),
          Port :: inet:port_number(),
          ApiKey :: int16(),
          ApiVersion :: int16(),
          ClientID :: kstring(),
          RequestPayload :: any(),
          Timeout :: timeout()) ->
                 {ok, ResponsePayload :: any()} | {error, Reason :: any()}.
crd(Host, Port, ApiKey, ApiVersion, ClientID, RequestPayload, Timeout) ->
    {ok, Socket} = kafka_socket:start_link(Host, Port, []),
    Result = kafka_socket:sync(Socket, ApiKey, ApiVersion, ClientID,
                               RequestPayload, Timeout),
    ok = kafka_socket:close(Socket),
    Result.
