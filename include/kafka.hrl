%%%----------------------------------------------------------------------
%%% File        : kafka.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : kafka definitions file
%%% Created     : 04 Aug 2014
%%%----------------------------------------------------------------------

-ifndef(_KAFKA).
-define(_KAFKA, true).

%% ----------------------------------------------------------------------
%% debugging

-ifdef(TRACE).
-define(
   trace(Format, Args),
   ok = io:format(
          "TRACE> mod:~w; line:~w; pid:~w; msg:" ++ Format ++ "~n",
          [?MODULE, ?LINE, self() | Args]
         )).
-else.
-define(trace(F, A), ok).
-endif.

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
