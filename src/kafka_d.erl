%%% @doc
%%% Apache Kafka API "for dummies".
%%%
%%% This module provides simplified wrappers for some useful
%%% requests to the Kafka broker.
%%%
%%% First argument of each function is a PID of socket process
%%% created by kafka_socket:start_link/3 function.
%%%
%%% I wrote this stuff in a separate module because all
%%% this functions are mostly version specific and may crash
%%% when Kafka protocol change.
%%% The module is compromise between lowlevel API provided by
%%% kafka_socket module and highlevel features usually provided
%%% by mature Kafka clients.
%%%
%%% I'm certain will not implement all possible and/or impossible
%%% wrappers here because Apache Kafka protocol is tricky and
%%% it can map to an infinite amount of "simple wrappers".

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 04 Apr 2016
%%% @copyright 2016, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka_d).

%% API exports
-export(
   [get_first_offset/3,
    get_last_offset/3,
    get_current_offset/4,
    set_current_offset/5,
    produce/5
   ]).

-include("kafka.hrl").
-include("kafka_types.hrl").
-include("kafka_constants.hrl").

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Fetch the very first available offset of the partition.
-spec get_first_offset(Socket :: pid(), topic_name(), partition_id()) ->
                              {ok, Offset :: offset()} |
                              {error, Reason :: any()}.
get_first_offset(Socket, Topic0, Partition) ->
    Topic = canonicalize_string(Topic0),
    case kafka_socket:sync(
           Socket, ?Offsets, _ApiVersion = 0, _ClientID = undefined,
           {_ReplicaID = -1, [{Topic, [{Partition, -2, _Len = 1}]}]},
           _SockReadTimeout = infinity) of
        {ok, [{Topic, [{Partition, ?NONE = _ErrorCode, [Offset]}]}]} ->
            {ok, Offset};
        {ok, [{Topic, [{Partition, ErrorCode, _}]}]} ->
            {error, ?err2atom(ErrorCode)};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Fetch the latest available offset of the partition.
-spec get_last_offset(Socket :: pid(), topic_name(), partition_id()) ->
                             {ok, Offset :: offset()} |
                             {error, Reason :: any()}.
get_last_offset(Socket, Topic0, Partition) ->
    Topic = canonicalize_string(Topic0),
    case kafka_socket:sync(
           Socket, ?Offsets, _ApiVersion = 0, _ClientID = undefined,
           {_ReplicaID = -1, [{Topic, [{Partition, -1, _Len = 1}]}]},
           _SockReadTimeout = infinity) of
        {ok, [{Topic, [{Partition, ?NONE = _ErrorCode, [Offset]}]}]} ->
            {ok, Offset};
        {ok, [{Topic, [{Partition, ErrorCode, _}]}]} ->
            {error, ?err2atom(ErrorCode)};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Fetch stored offset for GroupID in partition.
-spec get_current_offset(Socket :: pid(),
                         topic_name(),
                         partition_id(),
                         group_id()) ->
                                {ok, Offset :: offset()} |
                                {error, Reason :: any()}.
get_current_offset(Socket, Topic0, Partition, GroupID) ->
    Topic = canonicalize_string(Topic0),
    case kafka_socket:sync(
           Socket, ?OffsetFetch, _ApiVersion = 0, _ClientID = undefined,
           {GroupID, [{Topic, [Partition]}]},
           _SockReadTimeout = infinity) of
        {ok, [{Topic, [{Partition, Offset, _Metadata, ?NONE = _ErrorCode}]}]} ->
            {ok, Offset};
        {ok, [{Topic, [{Partition, _, _, ErrorCode}]}]} ->
            {error, ?err2atom(ErrorCode)};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Store offset for GroupID in partition.
-spec set_current_offset(Socket :: pid(),
                         topic_name(),
                         partition_id(),
                         group_id(),
                         offset()) ->
                                ok | {error, Reason :: any()}.
set_current_offset(Socket, Topic0, Partition, GroupID, Offset) ->
    Topic = canonicalize_string(Topic0),
    case kafka_socket:sync(
           Socket, ?OffsetCommit, _ApiVersion = 0, _ClientID = undefined,
           {GroupID, [{Topic, [{Partition, Offset, _Metadata = undefined}]}]},
           _SockReadTimeout = infinity) of
        {ok, [{_Topic, [{Partition, ?NONE = _ErrorCode}]}]} ->
            ok;
        {ok, [{_Topic, [{Partition, ErrorCode}]}]} ->
            {error, ?err2atom(ErrorCode)};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Post a single message to the partition.
-spec produce(Socket :: pid(),
              topic_name(),
              partition_id(),
              Key :: kafka:kbytes(),
              Value :: kafka:kbytes()) ->
                     ok | {error, Reason :: any()}.
produce(Socket, Topic0, Partition, Key, Value) ->
    Topic = canonicalize_string(Topic0),
    case kafka_socket:sync(
           Socket, ?Produce, _ApiVersion = 0, _ClientID = undefined,
           {_Acks = 1,
            _Timeout = 1000,
            [{Topic, [{Partition, [{_AutoAssignOffset = -1,
                                    {_MagicByte = 1,
                                     _Attributes = 0,
                                     Key, Value}}]}]}]},
           _SockReadTimeout = infinity) of
        {ok, [{_Topic, [{Partition, ?NONE = _ErrorCode, _BaseOffset}]}]} ->
            ok;
        {ok, [{_Topic, [{Partition, ErrorCode, _}]}]} ->
            {error, ?err2atom(ErrorCode)};
        {error, _Reason} = Error ->
            Error
    end.

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Convert string to a canonic form from the point
%% of view of protocol codec.
-spec canonicalize_string(binary() | string()) -> string().
canonicalize_string(Binary) when is_binary(Binary) ->
    binary_to_list(Binary);
canonicalize_string(List) when is_list(List) ->
    List.
