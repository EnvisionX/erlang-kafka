%%%----------------------------------------------------------------------
%%% File        : kafka_types.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : Apache Kafka Protocol type definitions
%%% Created     : 02 Apr 2016
%%%----------------------------------------------------------------------

-ifndef(_KAFKA_TYPES).
-define(_KAFKA_TYPES, true).

%% signed int8
-type int8() :: -128..127.

%% signed int16
-type int16() :: -32768..32767.

%% signed int32
-type int32() :: -2147483648..2147483647.

%% signed int64
-type int64() :: -9223372036854775808..9223372036854775807.

-type kstring() :: string() | undefined.

-type kbytes() :: binary() | undefined.

%% ----------------------------------------------------------------------
%% auxiliary types

-type broker_id() :: int32().

-type partition_id() :: int32().

-type offset() :: int64().

-type topic_name() :: string().

-type group_id() :: string().

-endif.
