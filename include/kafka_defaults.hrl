%%%----------------------------------------------------------------------
%%% File        : kafka_defaults.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : default values for configuration options
%%% Created     : 01 Apr 2016
%%%----------------------------------------------------------------------

-ifndef(_KAFKA_DEFAULTS).
-define(_KAFKA_DEFAULTS, true).

-define(CONNECT_TIMEOUT, 2000). %% two seconds
-define(CONNECT_RETRY_PERIOD, 2000). %% two seconds
-define(CONSUMER_GROUP_ID, "erlang-kafka-consumer").
-define(CONSUMER_INITIAL_OFFSET, current).
-define(CONSUMER_MAX_BYTES, 64 * 1024).
-define(CONSUMER_MAX_WAIT_TIME, 1000). %% one second
-define(CONSUMER_SLEEP_TIME, 5000). %% five seconds

-endif.
