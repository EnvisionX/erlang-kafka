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

-endif.
