%%% @doc
%%% Asynchronous consumer implementation.
%%%
%%% Connects to the Kafka cluster, consumes configured partition
%%% of a topic and send each received {Key, Value} pair to the
%%% target Erlang process in the format:
%%%
%%% {kafka_consumer,
%%%  ConsumerPID :: pid(),
%%%  msg,
%%%  TopicName :: kafka:topic_name(),
%%%  PartitionID :: kafka:partition_id(),
%%%  Offset :: kafka:offset(),
%%%  Key :: kafka:kbytes(),
%%%  Value :: kafka:kbytes()
%%% }
%%%
%%% Features:
%%% <ul>
%%%  <li>metadata discovery - consumer will automatically choose
%%%    Kafka node to consume from;</li>
%%%  <li>auto reconnect - consumer will re-discover and re-connect
%%%    on Kafka cluster configuration change;</li>
%%%  <li>offset autocommit mode.</li>
%%% </ul>
%%%
%%% Important note about message offsets. You can configure the
%%% process to consume from the eldest available message, from
%%% the head (latest available) of the partition, from current
%%% saved offset for the GroupID or from absolute offset. The
%%% messages will be consumed in order despite network failure
%%% and reconnect attempts inside the process. But if the process
%%% started without 'autocommit' option, it is up to you to save
%%% consumed offsets back to the Kafka cluster.
%%% Worth to say if you use 'autocommit' option, offsets will
%%% be marked as 'consumed' when they received from Kafka broker,
%%% but not when they really processed.
%%%
%%% Flow control.
%%% When listener became overloaded with data sent by consumer,
%%% it can call suspend/2 function to suspend consumpsion for
%%% a given period of time. Consumer will continue to fetch data
%%% from Kafka after requested time period.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 04 Apr 2016
%%% @copyright 2016, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka_consumer).

-behaviour(gen_server).

%% API exports
-export(
   [start_link/4,
    suspend/2,
    set_nodes/2,
    reconsume/2,
    close/1,
    reconnect/1
   ]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("kafka.hrl").
-include("kafka_defaults.hrl").
-include("kafka_types.hrl").
-include("kafka_constants.hrl").

%% --------------------------------------------------------------------
%% Internal signals
%% --------------------------------------------------------------------

%% to tell the consumer to suspend consumpsion for a while
-define(SUSPEND(Millis), {'*suspend*', Millis}).

%% to tell the consumer to update Kafka broker list
-define(SET_NODES(Nodes), {'*set_nodes*', Nodes}).

%% to tell the consumer to close the connection and exit
-define(CLOSE, '*close*').

%% to tell the consumer to re-establish the connection immediately
-define(RECONNECT, '*reconnect*').

%% to tell the process to re-consume starting from offset
-define(RECONSUME(Offset), {'*reconsume*', Offset}).

%% sent after 'disconnected' event received from socket process.
-define(CHECK_CONNECTION, '*check-connection*').

%% signal to start/continue data consuming
-define(FETCH, '*fetch*').

%% signal to disable suspend
-define(SUSPEND_DISABLE, '*suspend_disable*').

%% notification messages about consumer status change
-define(NOTIFY_CONNECT, connect).
-define(NOTIFY_DISCONNECT, disconnect).
-define(NOTIFY_SUSPEND, suspend).
-define(NOTIFY_RESUME, resume).

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-export_types(
   [knodes/0,
    options/0,
    option/0,
    listener/0,
    status_change_listener/0,
    status_change_message/0
   ]).

-type knodes() :: [{kafka_socket:host(), inet:port_number()}].

-type options() :: [option()].

-type option() ::
        autocommit |
        {offset, offset() | first | last | current} |
        {group_id, kstring()} |
        {max_bytes, pos_integer()} |
        {max_wait_time, pos_integer()} |
        {sleep_time, pos_integer()} |
        {connect_timeout, non_neg_integer()} |
        {reconnect_period, non_neg_integer()} |
        {listener, listener()} |
        {status_change_listener, status_change_listener()}.

-type listener() ::
        (RegisteredName :: atom()) |
        pid() |
        fun((Offset :: int64(), Key :: kbytes(), Value :: kbytes()) ->
                   Ignored :: any()).

-type status_change_listener() ::
        (RegisteredName :: atom()) |
        pid() |
        fun((status_change_message()) -> Ignored :: any()).

-type status_change_message() ::
        ?NOTIFY_CONNECT |
        ?NOTIFY_DISCONNECT |
        ?NOTIFY_SUSPEND |
        ?NOTIFY_RESUME.

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start a consumer as linked process.
-spec start_link(KafkaNodes :: knodes(),
                 TopicName :: kafka:kstring(),
                 PartitionID :: kafka:int32(),
                 Options :: options()) ->
                        {ok, Pid :: pid()} | {error, Reason :: any()}.
start_link(Nodes, TopicName, PartitionID, Options) ->
    Listener = proplists:get_value(listener, Options, self()),
    gen_server:start_link(
      ?MODULE, {Nodes, TopicName, PartitionID, Listener, Options}, []).

%% @doc Tell the process to suspend data consumpsion for a given
%% time period. This is kinda flow control for cases when data
%% listener is overloaded with data sent from consumer.
-spec suspend(pid(), Millis :: non_neg_integer()) -> ok.
suspend(Pid, Millis) ->
    gen_server:cast(Pid, ?SUSPEND(Millis)).

%% @doc Set list of Kafka broker nodes for bootstrapping.
%% If you interested in reconfiguring the consumer immediately, you
%% have to call reconnect/1 after it.
-spec set_nodes(pid(), knodes()) -> ok.
set_nodes(Pid, Nodes) ->
    gen_server:cast(Pid, ?SET_NODES(Nodes)).

%% @doc Tell the process to consume from another offset.
%% If offset is positive integer, it treated as absolute position.
%% If set as negative, it treated as current plus (actually minus) offset.
%% To make description more clear:
%% <ul>
%%  <li>reconsume(Socket, 1000) - means continue to consume from offset 1000;</li>
%%  <li>reconsume(Socket, -1000) - means reconsume last 1000 messages again.</li>
%% </ul>
-spec reconsume(pid(), Offset :: offset()) -> ok.
reconsume(Pid, Offset) ->
    gen_server:cast(Pid, ?RECONSUME(Offset)).

%% @doc Close the connection and stop the consumer process. Calling the
%% function for already terminated process is allowed.
-spec close(pid()) -> ok.
close(Pid) ->
    _Sent = Pid ! ?CLOSE,
    ok.

%% @doc Force the process to re-establish connection to the broker.
-spec reconnect(pid()) -> ok.
reconnect(Pid) ->
    gen_server:cast(Pid, ?RECONNECT).

%% ----------------------------------------------------------------------
%% gen_server callbacks
%% ----------------------------------------------------------------------

-record(
   state,
   {nodes :: knodes(),
    topic :: kstring(),
    partition :: int32(),
    group_id :: kstring(),
    offset :: offset() | undefined,
    listener :: listener(),
    status_change_listener :: status_change_listener() | undefined,
    autocommit :: boolean(),
    max_bytes :: pos_integer(),
    max_wait_time :: pos_integer(),
    sleep_time :: pos_integer(),
    opts :: options(),
    suspended = false :: boolean(),
    suspend_timer :: reference() | undefined,
    socket :: pid() | undefined
   }).

%% @hidden
-spec init({knodes(), kstring(), int32(), listener(), options()}) ->
                  {ok, InitialState :: #state{}} |
                  {stop, Reason :: any()}.
init({Nodes, TopicName, PartitionID, Listener, Options} = _Args) ->
    ?trace("init(~9999p)", [_Args]),
    false = process_flag(trap_exit, true),
    ok = reconnect(self()),
    {ok,
     #state{
        nodes = Nodes,
        topic = TopicName,
        partition = PartitionID,
        group_id = proplists:get_value(group_id, Options, ?CONSUMER_GROUP_ID),
        listener = Listener,
        status_change_listener = proplists:get_value(status_change_listener, Options),
        autocommit = lists:member(autocommit, Options),
        max_bytes = proplists:get_value(max_bytes, Options, ?CONSUMER_MAX_BYTES),
        max_wait_time =
            proplists:get_value(max_wait_time, Options, ?CONSUMER_MAX_WAIT_TIME),
        sleep_time =
            proplists:get_value(sleep_time, Options, ?CONSUMER_SLEEP_TIME),
        opts = Options}}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(?RECONNECT, State) ->
    {noreply, connect(State)};
handle_cast(?RECONSUME(Offset), State) when Offset < 0 ->
    {noreply, State#state{offset = State#state.offset + Offset}};
handle_cast(?RECONSUME(Offset), State) ->
    {noreply, State#state{offset = Offset}};
handle_cast(?SET_NODES(Nodes), State) ->
    ?trace("node list changed from ~9999p to ~9999p",
           [State#state.nodes, Nodes]),
    {noreply, State#state{nodes = Nodes}};
handle_cast(?SUSPEND(Millis), State)
  when State#state.suspended ->
    %% Suspend requested, but we're already in suspend.
    %% Reschedule resume as owner wants.
    {ok, cancel} = timer:cancel(State#state.suspend_timer),
    {ok, TRef} = timer:send_after(Millis, ?SUSPEND_DISABLE),
    {noreply, State#state{suspend_timer = TRef}};
handle_cast(?SUSPEND(Millis), State) ->
    %% Enable suspend mode.
    ok = notify(State, ?NOTIFY_SUSPEND),
    {ok, TRef} = timer:send_after(Millis, ?SUSPEND_DISABLE),
    {noreply,
     State#state{
       suspended = true,
       suspend_timer = TRef
      }};
handle_cast(_Request, State) ->
    ?trace("unknown cast message: ~9999p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_info(Info :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), #state{}}.
handle_info(?FETCH, State) when State#state.suspended ->
    %% we're in suspend mode, do not consume, wait
    %% for suspend disable
    {ok, _TRef} = timer:send_after(State#state.sleep_time, ?FETCH),
    {noreply, State};
handle_info(?FETCH, State) ->
    {noreply, fetch(State)};
handle_info(?SUSPEND_DISABLE, State) ->
    ok = notify(State, ?NOTIFY_RESUME),
    {noreply,
     State#state{
       suspended = false,
       suspend_timer = undefined
      }};
handle_info({kafka_socket, Socket, connected}, State)
  when State#state.socket == Socket ->
    ok = notify(State, ?NOTIFY_CONNECT),
    {noreply, State};
handle_info({kafka_socket, Socket, disconnected}, State)
  when State#state.socket == Socket ->
    ok = notify(State, ?NOTIFY_DISCONNECT),
    %% For some reason socket process closed TCP connection.
    %% Schedule connection state check after two periods of
    %% reconnect. If connection will not restore until this
    %% deadline, we will rediscover replicas from scratch.
    Period =
        proplists:get_value(
          reconnect_period, State#state.opts, ?CONNECT_RETRY_PERIOD),
    {ok, _TRef} = timer:send_after(Period * 2, ?CHECK_CONNECTION),
    {noreply, State};
handle_info(?CHECK_CONNECTION, #state{socket = undefined} = State) ->
    %% bootstrapping is in progress, ignore it
    {noreply, State};
handle_info(?CHECK_CONNECTION, State) ->
    case kafka_socket:connected(State#state.socket) of
        true ->
            %% it's all right, connection restored
            {noreply, State};
        false ->
            %% Two periods of reconnection elapsed but socket
            %% process still not connected to the broker.
            %% Initiate bootstrap from scratch.
            {noreply, connect(State)}
    end;
handle_info(?CLOSE, State) ->
    {stop, normal, disconnect(State)};
handle_info({'EXIT', PID, _Reason}, State)
  when State#state.socket == PID ->
    ?trace("socket process died: ~9999p", [_Reason]),
    {noreply, connect(State)};
handle_info(_Request, State) ->
    ?trace("unknown info message: ~9999p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    ?trace("unknown call from ~w: ~9999p", [_From, _Request]),
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, State) ->
    _NewState = disconnect(State),
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Close the connection to the broker.
-spec disconnect(State :: #state{}) -> NewState :: #state{}.
disconnect(#state{socket = undefined} = State) ->
    %% already disconnected
    State;
disconnect(State) ->
    ?trace("disconnecting", []),
    ok = kafka_socket:close(State#state.socket),
    State#state{socket = undefined}.

%% @doc Open connection for consuming. This includes discovering all
%% Kafka cluster nodes and select one having replica of the configured
%% partition.
-spec connect(#state{}) -> #state{}.
connect(State0) ->
    State = disconnect(State0),
    case kafka:metadata(State#state.nodes) of
        {Brokers, TopicMetadata} ->
            Metadata =
                [{LeaderBrokerID, ReplicaBrokerIDs, InSyncBrokerIDs} ||
                    {?NONE = _TopicErrorCode, TopicName, Partitions} <- TopicMetadata,
                    TopicName == State#state.topic,
                    {?NONE = _PartitionErrorCode, PartitionID, LeaderBrokerID,
                     ReplicaBrokerIDs, InSyncBrokerIDs} <- Partitions,
                    PartitionID == State#state.partition],
            case Metadata of
                [{Leader, Replicas, InSyncReplicas}] ->
                    case connect_replica(State, Brokers, Leader,
                                         Replicas, InSyncReplicas) of
                        {ok, Socket} ->
                            case resolve_offset(State#state{socket = Socket}) of
                                #state{socket = undefined} = State1 ->
                                    %% disconnected during offset resolving.
                                    %% do nothing
                                    State1;
                                State1 ->
                                    %% still connected. Schedule fetch
                                    _Sent = self() ! ?FETCH,
                                    State1
                            end;
                        error ->
                            %% there are no replicas online
                            ok = schedule_reconnect(State),
                            State
                    end;
                [] ->
                    %% no such topic and/or partition
                    ok = schedule_reconnect(State),
                    State
            end;
        undefined ->
            ok = schedule_reconnect(State),
            State
    end.

%% @doc Convert configuration value {offset,x} (which can take
%% values like 'first', 'last', 'current') to real counter
%% in the state term.
-spec resolve_offset(#state{}) -> #state{}.
resolve_offset(#state{offset = Offset} = State)
  when is_integer(Offset) ->
    %% already resolved
    check_offset(State);
resolve_offset(State) ->
    Options = State#state.opts,
    Socket = State#state.socket,
    Topic = State#state.topic,
    Partition = State#state.partition,
    case proplists:get_value(offset, Options, ?CONSUMER_INITIAL_OFFSET) of
        Offset when is_integer(Offset) ->
            ?trace("offset explicitly set to ~w", [Offset]),
            check_offset(State#state{offset = Offset});
        first ->
            case kafka_d:get_first_offset(Socket, Topic, Partition) of
                {ok, First} ->
                    ?trace("first offset is ~w", [First]),
                    State#state{offset = First};
                {error, _Reason} ->
                    ok = schedule_reconnect(State),
                    disconnect(State)
            end;
        current ->
            case get_current_offset(State) of
                {ok, Current} ->
                    ?trace("current offset is ~w", [Current]),
                    check_offset(State#state{offset = Current});
                error ->
                    ok = schedule_reconnect(State),
                    disconnect(State)
            end;
        last ->
            case kafka_d:get_last_offset(Socket, Topic, Partition) of
                {ok, Last} ->
                    ?trace("last offset is ~w", [Last]),
                    State#state{offset = Last};
                {error, _Reason} ->
                    ok = schedule_reconnect(State),
                    disconnect(State)
            end
    end.

%% @doc Get current offset for the GroupID. Fallback to the very first
%% offset if no offset was stored for the GroupID.
-spec get_current_offset(#state{}) -> {ok, offset()} | error.
get_current_offset(State) ->
    Socket = State#state.socket,
    Topic = State#state.topic,
    Partition = State#state.partition,
    GroupID = State#state.group_id,
    case kafka_socket:sync(
           Socket, ?OffsetFetch, _ApiVersion = 0, _ClientID = undefined,
           {GroupID, [{Topic, [Partition]}]},
           _SockReadTimeout = infinity) of
        {ok, [{Topic, [{Partition, Offset, _Metadata, ?NONE = _ErrorCode}]}]} ->
            {ok, Offset};
        {ok, [{_Topic, [{_Partition, _, _, ?UNKNOWN_TOPIC_OR_PARTITION}]}]} ->
            %% WTF. This error is reported when no offset is found for
            %% the GroupID.
            case kafka_d:get_first_offset(Socket, Topic, Partition) of
                {ok, First} ->
                    ?trace("first offset is ~w", [First]),
                    {ok, First};
                {error, _Reason} ->
                    error
            end;
        {error, _Reason} ->
            error
    end.

%% @doc Check if resolved offset is in valid bounds and fix it if not.
-spec check_offset(#state{}) -> #state{}.
check_offset(#state{offset = Offset} = State) when is_integer(Offset) ->
    Socket = State#state.socket,
    Topic = State#state.topic,
    Partition = State#state.partition,
    case kafka_d:get_first_offset(Socket, Topic, Partition) of
        {ok, First} ->
            ?trace("the eldest offset is ~w", [First]),
            if Offset < First ->
                    State#state{offset = First};
               true -> State
            end;
        {error, _Reason} ->
            ok = schedule_reconnect(State),
            disconnect(State)
    end.

%% @doc Schedule reconnect after configured period of time.
-spec schedule_reconnect(#state{}) -> ok.
schedule_reconnect(State) ->
    Period =
        proplists:get_value(
          reconnect_period, State#state.opts, ?CONNECT_RETRY_PERIOD),
    ?trace("reconnect scheduled after ~w millis", [Period]),
    {ok, _TRef} = timer:apply_after(Period, ?MODULE, reconnect, [self()]),
    ok.

-type brokers() ::
        [{BrokerID :: broker_id(),
          BrokerHost :: kafka_socket:host(),
          BrokerPort :: inet:port_number()}].

%% @doc Try to connect to one of partition's replica node.
%% First in-sync replicas will be tried, then follower replicas,
%% and then the leader of the partition.
-spec connect_replica(#state{},
                      brokers(),
                      Leader :: broker_id(),
                      Replicas :: [broker_id()],
                      InSyncReplicas :: [broker_id()]) ->
                             {ok, Socket :: pid()} | error.
connect_replica(State, Brokers, Leader, [], []) ->
    {Leader, Host, Port} = lists:keyfind(Leader, 1, Brokers),
    case sync_connect(State, Host, Port) of
        {ok, _Socket} = Ok ->
            ?trace("connected to leader #~w", [Leader]),
            Ok;
        error ->
            error
    end;
connect_replica(State, Brokers, Leader, Replicas, []) ->
    {Replica, ReplicasTail} = pop_random(Replicas),
    {Replica, Host, Port} = lists:keyfind(Replica, 1, Brokers),
    case sync_connect(State, Host, Port) of
        {ok, _Socket} = Ok ->
            ?trace("connected to out-of-sync replica #~w", [Replica]),
            Ok;
        error ->
            connect_replica(
              State, Brokers, Leader, ReplicasTail, [])
    end;
connect_replica(State, Brokers, Leader, Replicas, InSyncReplicas) ->
    {Replica, InSyncReplicasTail} = pop_random(InSyncReplicas),
    {Replica, Host, Port} = lists:keyfind(Replica, 1, Brokers),
    case sync_connect(State, Host, Port) of
        {ok, _Socket} = Ok ->
            ?trace("connected to in-sync replica #~w", [Replica]),
            Ok;
        error ->
            connect_replica(
              State, Brokers, Leader, Replicas -- [Replica],
              InSyncReplicasTail)
    end.

%% @doc Try to start Kafka socket process synchronously.
-spec sync_connect(#state{}, kafka_socket:host(), inet:port_number()) ->
                          {ok, Socket :: pid()} | error.
sync_connect(State, Host, Port) ->
    ConnOptions =
        [reconnect,
         sync_start,
         {state_listener, self()}] ++
        [E || {K, _} = E <- State#state.opts,
              lists:member(K, [connect_timeout, reconnect_period])],
    try kafka_socket:start_link(Host, Port, ConnOptions) of
        {ok, _Socket} = Ok ->
            Ok;
        _Other ->
            ?trace("failed to connect to ~9999p:~9999p"
                   " with opts ~9999p: ~9999p",
                   [Host, Port, ConnOptions, _Other]),
            error
    catch
        _ExcType:_ExcReason ->
            ?trace("failed to connect to ~9999p:~9999p"
                   " with opts ~9999p: crashed. ~9999p",
                   [Host, Port, ConnOptions,
                    [{type, _ExcType},
                     {reason, _ExcReason},
                     {stacktrace, erlang:get_stacktrace()}]]),
            error
    end.

%% @doc Pop random element from the list.
%% Return the element and result list with the element extracted.
-spec pop_random(list()) -> {Elem :: any(), NewList :: list()}.
pop_random([Elem]) ->
    {Elem, []};
pop_random(List) ->
    _ = get(random_seed) == undefined andalso random:seed(os:timestamp()),
    Index = random:uniform(length(List)),
    {List1, [Elem | List2]} = lists:split(Index - 1, List),
    {Elem, List1 ++ List2}.

%% @doc Issue a Fetch request to the connected Kafka broker.
-spec fetch(#state{}) -> #state{}.
fetch(#state{socket = undefined} = State) ->
    %% not connected
    State;
fetch(#state{socket = Socket} = State) ->
    Topic = State#state.topic,
    Partition = State#state.partition,
    FetchRequest =
        {_ReplicaID = -1, %% used only for s2s links
         _MaxWaitTimeMillis = State#state.max_wait_time,
         _MinBytes = 1,
         [{Topic, [{Partition, State#state.offset, State#state.max_bytes}]}]},
    ?trace("consuming from offset ~w", [State#state.offset]),
    case kafka_socket:sync(Socket, _ApiKey = ?Fetch, _ApiVersion = 0,
                           _ClientID = undefined, FetchRequest,
                           _SockReadTimeout = infinity) of
        {ok, [{_Topic, [{_Partition, ?NONE = _ErrorCode, _HighWatermark, []}]}]} ->
            ?trace("broker returned empty message set", []),
            {ok, _TRef} = timer:send_after(State#state.sleep_time, ?FETCH),
            State;
        {ok, [{_Topic, [{_Partition, ?NONE = _ErrorCode,
                         HighWatermark, MessageSet}]}]} ->
            MaxOffset =
                lists:foldl(
                  fun({Offset, {_MagicByte, _Attrs, Key, Value}}, Accum) ->
                          Listener = State#state.listener,
                          if is_function(Listener, 3) ->
                                  Listener(Offset, Key, Value);
                             true ->
                                  Msg = {?MODULE, self(), msg, Topic,
                                         Partition, Offset, Key, Value},
                                  _Sent = Listener ! Msg
                          end,
                          max(Offset, Accum)
                  end, State#state.offset, MessageSet),
            NextOffset = MaxOffset + 1,
            _Ignored = State#state.autocommit andalso commit_offset(State, NextOffset),
            if HighWatermark < NextOffset ->
                    %% End of the partition reached.
                    %% Sleep for some time.
                    {ok, _TRef} = timer:send_after(State#state.sleep_time, ?FETCH),
                    ok;
               true ->
                    %% There are more messages in the partition.
                    %% Schedule next fetch ASAP
                    _Sent = self() ! ?FETCH,
                    ok
            end,
            State#state{offset = NextOffset};
        {ok, [{_Topic, [{_Partition, ?OFFSET_OUT_OF_RANGE = _ErrorCode, _, _}]}]} ->
            ?trace("fetch failed: ~w", [?err2atom(_ErrorCode)]),
            case kafka_d:get_last_offset(Socket, Topic, Partition) of
                {ok, Last} when Last < State#state.offset ->
                    ?trace("head of the partition reached at ~w", [Last]),
                    {ok, _TRef} = timer:send_after(State#state.sleep_time, ?FETCH),
                    State;
                {ok, _Last} ->
                    %% bad min bound?
                    _Sent = self() ! ?FETCH,
                    check_offset(State);
                {error, _Reason} ->
                    ok = schedule_reconnect(State),
                    disconnect(State)
            end;
        {ok, [{_Topic, [{_Partition, _ErrorCode, _, _}]}]} ->
            ?trace("fetch failed: ~w", [?err2atom(_ErrorCode)]),
            {ok, _TRef} = timer:send_after(1000, ?FETCH),
            State;
        {error, _Reason} ->
            ok = reconnect(self()),
            disconnect(State)
    end.

%% @doc Store current cunsumed offset to the Kafka broker.
-spec commit_offset(#state{}, offset()) -> ok.
commit_offset(State, Offset) ->
    _Ignored =
        kafka_d:set_current_offset(State#state.socket,
                                   State#state.topic,
                                   State#state.partition,
                                   State#state.group_id,
                                   Offset),
    ok.

%% @doc Send notification to status change listener process, if
%% such defined.
-spec notify(#state{}, status_change_message()) -> ok.
notify(#state{status_change_listener = Listener}, Message)
  when is_function(Listener, 1) ->
    try
        _Ignored = Listener(Message),
        ok
    catch
        _ExcType:_ExcReason ->
            ok
    end;
notify(#state{status_change_listener = Listener,
              topic = Topic,
              partition = Partition}, Message)
  when Listener /= undefined ->
    try
        _Sent = Listener ! {?MODULE, self(), Topic, Partition, Message},
        ok
    catch
        _ExcType:_ExcReason ->
            ok
    end;
notify(_State, _Message) ->
    ok.

%% ----------------------------------------------------------------------
%% Unit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

pop_random_test_() ->
    [?_assertMatch({1, []}, pop_random([1])),
     fun() ->
             List = lists:sort([1, 2, 3, 4, 5]),
             {[], List2} =
                 lists:foldl(
                   fun(_, {Src, Dst}) ->
                           {Elem, Src2} = pop_random(Src),
                           ?assert(lists:member(Elem, List)),
                           ?assertNot(lists:member(Elem, Src2)),
                           {Src2, [Elem | Dst]}
                   end, {List, []}, lists:seq(1, length(List))),
             ?assertMatch(List, lists:sort(List2))
     end].

-endif.
