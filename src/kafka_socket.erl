%%% @doc
%%% Kafka broker connection implementation.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 01 Apr 2016
%%% @copyright 2016, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka_socket).

%% API exports
-export(
   [start_link/3,
    connected/1,
    close/1,
    reconnect/1,
    sync/5, sync/6,
    async/5
   ]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("kafka.hrl").
-include("kafka_types.hrl").
-include("kafka_defaults.hrl").

%% --------------------------------------------------------------------
%% Internal signals
%% --------------------------------------------------------------------

%% to tell the client to close the connection and exit
-define(CLOSE, '*close*').

%% to tell the client to re-establish the connection immediately
-define(RECONNECT, '*reconnect*').

-define(KAFKA_SYNC(Request, Timeout), {'*sync*', Request, Timeout}).

-define(KAFKA_ASYNC(Request), {'*async*', Request}).

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-export_types(
   [host/0,
    options/0,
    option/0
   ]).

-type host() ::
        (HostnameOrIP :: nonempty_string() | atom() | binary()) |
        inet:ip_address().

-type options() :: [option()].

-type option() ::
        reconnect |
        {connect_timeout, non_neg_integer()} |
        {reconnect_period, non_neg_integer()}.

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start a linked process. It will connect to the broker in
%% background.
-spec start_link(Host :: host(), Port :: inet:port_number(),
                 Options :: options()) ->
                        {ok, Pid :: pid()} | {error, Reason :: any()}.
start_link(Host, Port, Options) ->
    gen_server:start_link(?MODULE, _Args = {Host, Port, Options}, []).

%% @doc Return 'true' if connection is established and 'false' otherwise.
-spec connected(pid()) -> boolean().
connected(Pid) ->
    %% It was done in such non-usual manner to not block this
    %% request if the process is busy by network transfers.
    {dictionary, List} = process_info(Pid, dictionary),
    lists:keyfind(connected, 1, List) /= false.

%% @doc Close the connection. Calling the function for already terminated
%% process is allowed.
-spec close(pid()) -> ok.
close(Pid) ->
    _Sent = Pid ! ?CLOSE,
    ok.

%% @doc Force the process to re-establish connection to the broker.
-spec reconnect(pid()) -> ok.
reconnect(Pid) ->
    gen_server:cast(Pid, ?RECONNECT).

%% @equiv request(Pid, ApiKey, ApiVersion, ClientID, RequestPayload, infinity)
%% @doc Send synchronous request to the broker, receive and decode response.
%% This function requires decoded request payload structure as argument
%% and return decoded response payload structure on success.
-spec sync(pid(),
           ApiKey :: int16(),
           ApiVersion :: int16(),
           ClientID :: kstring(),
           RequestPayload :: any()) ->
                  {ok, ResponsePayload :: tuple()} | {error, Reason :: any()}.
sync(Pid, ApiKey, ApiVersion, ClientID, RequestPayload) ->
    sync(Pid, ApiKey, ApiVersion, ClientID, RequestPayload, infinity).

%% @doc Send synchronous request to the broker, receive and decode response.
%% This function requires decoded request payload structure as argument
%% and return decoded response payload structure on success.
-spec sync(pid(),
           ApiKey :: int16(),
           ApiVersion :: int16(),
           ClientID :: kstring(),
           RequestPayload :: any(),
           Timeout :: timeout()) ->
                  {ok, ResponsePayload :: tuple()} | {error, Reason :: any()}.
sync(Pid, ApiKey, ApiVersion, ClientID, RequestPayload, Timeout) ->
    Req = encode_request(ApiKey, ApiVersion, ClientID, RequestPayload),
    case gen_server:call(Pid, ?KAFKA_SYNC(Req, Timeout), infinity) of
        {ok, Encoded} ->
            Spec = kafka_api:resp(ApiKey, ApiVersion),
            {{_CorellationID, Decoded}, _Tail} = kafka_codec:dec(Spec, Encoded),
            {ok, Decoded};
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Send asynchronous request to the broker.
%% This is a special case of the Apache Kafka protocol and can be used
%% only in rare cases. One of them is a Produce request with RequiredAcks
%% field set to 0.
%% The broker doesn't send response for such requests. So, if you try
%% to issue such Produce request with normal request/5 function, it will
%% block waiting for response from te broker until timeout or until
%% TCP connection close.
%% Although you can use this function to issue any kind of request
%% keeping in mind that if the broker will send response, it will be
%% silently discarded.
-spec async(pid(),
            ApiKey :: int16(),
            ApiVersion :: int16(),
            ClientID :: kstring(),
            RequestPayload :: any()) ->
                   ok | {error, Reason :: any()}.
async(Pid, ApiKey, ApiVersion, ClientID, RequestPayload) ->
    Req = encode_request(ApiKey, ApiVersion, ClientID, RequestPayload),
    gen_server:call(Pid, ?KAFKA_ASYNC(Req), infinity).

%% ----------------------------------------------------------------------
%% gen_server callbacks
%% ----------------------------------------------------------------------

-record(
   state,
   {host :: host(),
    port :: inet:port_number(),
    reconnect :: boolean(),
    opts = [] :: options(),
    socket :: port()
   }).

%% @hidden
-spec init({host(), inet:port_number(), options()}) ->
                  {ok, InitialState :: #state{}} |
                  {stop, Reason :: any()}.
init({Host, Port, Options}) ->
    ?trace("init(~9999p)", [{Host, Port, Options}]),
    Reconnect = lists:member(reconnect, Options),
    State0 =
        #state{
           host = Host,
           port = Port,
           reconnect = Reconnect,
           opts = Options},
    if Reconnect ->
            %% schedule reconnect in background
            ok = reconnect(self()),
            {ok, State0};
       true ->
            %% try to connect right here
            case connect(State0) of
                {ok, Socket} ->
                    {ok, State0#state{socket = Socket}};
                {error, Reason} ->
                    {stop, Reason}
            end
    end.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(?RECONNECT, State) ->
    State1 = disconnect(State),
    case connect(State1) of
        {ok, Socket} ->
            {noreply, State1#state{socket = Socket}};
        {error, _Reason} when State1#state.reconnect ->
            {noreply, State1};
        {error, Reason} ->
            {stop, {reconnect_failed, Reason}, State1}
    end;
handle_cast(_Request, State) ->
    ?trace("unknown cast message:~n\t~p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_info(Info :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}} |
                         {stop, Reason :: any(), #state{}}.
handle_info({tcp_closed, Socket}, State)
  when Socket == State#state.socket ->
    ?trace("connection closed", []),
    _OldStatus = erase(connected),
    State1 = State#state{socket = undefined},
    if State#state.reconnect ->
            case connect(State1) of
                {ok, NewSocket} ->
                    {noreply, State1#state{socket = NewSocket}};
                {error, _Reason} ->
                    {noreply, State1}
            end;
       true ->
            {stop, tcp_closed, State1}
    end;
handle_info({tcp_error, Socket, Reason}, State)
  when Socket == State#state.socket ->
    ?trace("tcp_error occured: ~9999p", [Reason]),
    State1 = disconnect(State),
    if State#state.reconnect ->
            case connect(State1) of
                {ok, NewSocket} ->
                    {noreply, State1#state{socket = NewSocket}};
                {error, _Reason} ->
                    {noreply, State1}
            end;
       true ->
            {stop, {tcp_error, Reason}, State1}
    end;
handle_info({tcp, Socket, _Data}, State)
  when Socket == State#state.socket ->
    ?trace("unexpected socket data: ~9999p", [_Data]),
    {noreply, State};
handle_info(?CLOSE, State) ->
    {stop, normal, disconnect(State)};
handle_info(_Request, State) ->
    ?trace("unknown info message:~n\t~p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {reply, Reply :: any(), NewState :: #state{}} |
                         {noreply, NewState :: #state{}}.
handle_call(?KAFKA_SYNC(_, _), _From, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};
handle_call(?KAFKA_ASYNC(_), _From, #state{socket = undefined} = State) ->
    {reply, {error, not_connected}, State};
handle_call(?KAFKA_SYNC(Request, Timeout), _From, State) ->
    Reply =
        try
            handle_sync(State, Request, Timeout)
        catch
            ExcType:ExcReason ->
                {error,
                 {crashed,
                  [{type, ExcType},
                   {reason, ExcReason},
                   {stacktrace, erlang:get_stacktrace()},
                   {request, Request}]}}
        end,
    case Reply of
        {error, _} ->
            ok = reconnect(self()),
            {reply, Reply, disconnect(State)};
        _ ->
            {reply, Reply, State}
    end;
handle_call(?KAFKA_ASYNC(Request), _From, State) ->
    Reply =
        try
            handle_async(State, Request)
        catch
            ExcType:ExcReason ->
                {error,
                 {crashed,
                  [{type, ExcType},
                   {reason, ExcReason},
                   {stacktrace, erlang:get_stacktrace()},
                   {request, Request}]}}
        end,
    case Reply of
        {error, _} ->
            ok = reconnect(self()),
            {reply, Reply, disconnect(State)};
        _ ->
            {reply, Reply, State}
    end;
handle_call(_Request, _From, State) ->
    ?trace("unknown call from ~w:~n\t~p", [_From, _Request]),
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Encode Apache Kafka request message for network transfer.
%% Helper for API functions: sync/6, async/5.
-spec encode_request(ApiKey :: int16(),
                     ApiVersion :: int16(),
                     ClientID :: kstring(),
                     RequestPayload :: any()) ->
                            binary().
encode_request(ApiKey, ApiVersion, ClientID, RequestPayload) ->
    _ = get(random_seed) == undefined andalso random:seed(os:timestamp()),
    CorellationID = random:uniform(16#100000000) - 16#7FFFFFFF,
    FullRequest = {ApiKey, ApiVersion, CorellationID, ClientID, RequestPayload},
    Spec = kafka_api:req(ApiKey, ApiVersion),
    iolist_to_binary(kafka_codec:enc(Spec, FullRequest)).

%% @doc Close the connection to the broker.
-spec disconnect(State :: #state{}) -> NewState :: #state{}.
disconnect(#state{socket = undefined} = State) ->
    %% already disconnected
    State;
disconnect(State) ->
    ?trace("disconnecting from ~w",
           [element(2, inet:peername(State#state.socket))]),
    ok = gen_tcp:close(State#state.socket),
    _OldStatus = erase(connected),
    State#state{socket = undefined}.

%% @doc Try to establish connection to the broker.
-spec connect(State :: #state{}) ->
                     {ok, Socket :: port()} | {error, Reason :: any()}.
connect(State) ->
    Host = State#state.host,
    Port = State#state.port,
    Options = [binary, {active, true}, {keepalive, true},
               %% According to Apache Kafka protocol, each
               %% request/response message is prepended with its length
               %% encoded as SIGNED big-endian int32.
               %% But Erlang uses UNSIGNED big-endian as packet header.
               %% Hope there never occur messages bigger than 2GB :)
               {packet, 4}],
    ConnectTimeout =
        proplists:get_value(
          connect_timeout, State#state.opts, ?CONNECT_TIMEOUT),
    ?trace("connecting to ~9999p:~w with opts ~9999p...",
           [Host, Port, Options]),
    case gen_tcp:connect(Host, Port, Options, ConnectTimeout) of
        {ok, Socket} ->
            _OldStatus = put(connected, true),
            ?trace("connected to ~9999p:~w", [Host, Port]),
            {ok, Socket};
        {error, _Reason} = Error ->
            ?trace("failed to connect to ~9999p:~w: ~9999p",
                   [Host, Port, _Reason]),
            if State#state.reconnect ->
                    Period =
                        proplists:get_value(
                          reconnect_period, State#state.opts,
                          ?CONNECT_RETRY_PERIOD),
                    ?trace("reconnect scheduled after ~w millis", [Period]),
                    {ok, _TRef} =
                        timer:apply_after(
                          Period, ?MODULE, reconnect, [self()]), ok;
               true ->
                    ok
            end,
            Error
    end.

%% @doc Perform synchronous request to the connected broker.
-spec handle_sync(#state{}, Request :: binary(), timeout()) ->
                         {ok, Response :: binary()} | {error, Reason :: any()}.
handle_sync(State, Request, Timeout) ->
    <<_:32/integer, CorellationID:32/big-signed, _/binary>> = Request,
    Socket = State#state.socket,
    ?trace("sending to ~w 0x~4.16.0B bytes with CorrelationID=~w: ~s",
           [element(2, inet:peername(Socket)),
            size(Request), CorellationID, base64:encode(zlib:gzip(Request))]),
    case gen_tcp:send(Socket, Request) of
        ok ->
            receive
                {tcp, Socket, <<CorellationID:32/big-signed, _/binary>> = Response} ->
                    ?trace("encoded response: ~s",
                           [base64:encode(zlib:gzip(Response))]),
                    {ok, Response};
                {tcp, _Socket, <<_BadCorellationID:32/big-signed, _/binary>> = _Resp} ->
                    ?trace("encoded response: ~s",
                           [base64:encode(zlib:gzip(_Resp))]),
                    ?trace(
                       "corellation ID mismatch: ~w /= ~w",
                       [CorellationID, _BadCorellationID]),
                    {error, corellation_id_mismatch};
                {tcp_closed, Socket} ->
                    ?trace("failed to receive the response: tcp_closed", []),
                    {error, tcp_closed};
                {tcp_error, Socket, Reason} ->
                    ?trace(
                       "failed to receive the response: ~9999p", [Reason]),
                    {error, Reason}
            after Timeout ->
                    ?trace("failed to receive the response: timeout", []),
                    {error, timeout}
            end;
        {error, Reason} ->
            ?trace("failed to send request: ~9999p", [Reason]),
            {error, Reason}
    end.

%% @doc Perform asynchronous request to the connected broker.
-spec handle_async(#state{}, Request :: binary()) ->
                          {ok, Response :: binary()} | {error, Reason :: any()}.
handle_async(State, Request) ->
    <<_:32/integer, _CorellationID:32/big-signed, _/binary>> = Request,
    Socket = State#state.socket,
    ?trace("sending async to ~w 0x~4.16.0B bytes with CorrelationID=~w: ~s",
           [element(2, inet:peername(Socket)),
            size(Request), _CorellationID, base64:encode(zlib:gzip(Request))]),
    case gen_tcp:send(Socket, Request) of
        ok ->
            ok;
        {error, Reason} ->
            ?trace("failed to send request: ~9999p", [Reason]),
            {error, Reason}
    end.
