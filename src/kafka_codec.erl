%%% @doc
%%% Apache Kafka protocol codec.
%%%
%%% The codec is used to lowlevel encoding/decoding of Apache Kafka
%%% request/response messages and any of its parts.
%%%
%%% To encode or decode the message a specification of the message
%%% must be supplied as first argument of encoding/decoding function.

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 02 Apr 2016
%%% @copyright 2016, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(kafka_codec).

%% API exports
-export([enc/2, dec/2]).

-include("kafka.hrl").

-export_types(
   [ktype/0,
    ksimple/0,
    karray/0,
    kseq/0,
    kmset/0,
    lmsg/0
   ]).

%% Apache Kafka message specification:
-type ktype() :: ksimple() | karray() | kseq() | kmset() | kmsg().
%% Simple types:
-type ksimple() :: int8 | int16 | int32 | int64 | string | bytes.
%% Compound types: array of elements of the same type:
-type karray() :: [ktype()].
%% Compound types: sequence of elements of different types:
-type kseq() :: {[ktype()]}.
%% Compound types: special case for MessageSet array, which is
%% actually encoded as bytes (without int32 header with array length
%% but with int32 header with byte length).
%% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
-type kmset() :: {mset, kseq()}.
%% Message is encoded and prepended with int32 size and int32 CRC
-type kmsg() :: {msg, ktype()}.

%% ----------------------------------------------------------------------
%% API functions
%% ----------------------------------------------------------------------

%% @doc Encode term representing Kafka request/response message
%% according to specification.
-spec enc(Spec :: ktype(), Term :: any()) -> iolist().
enc(int8 = _K, I) ->
    ?trace("enc(~9999p, ~9999p)", [_K, I]),
    <<I:8/big-signed>>;
enc(int16 = _K, I) ->
    ?trace("enc(~9999p, ~9999p)", [_K, I]),
    <<I:16/big-signed>>;
enc(int32 = _K, I) ->
    ?trace("enc(~9999p, ~9999p)", [_K, I]),
    <<I:32/big-signed>>;
enc(int64 = _K, I) ->
    ?trace("enc(~9999p, ~9999p)", [_K, I]),
    <<I:64/big-signed>>;
enc(string = _K, undefined = _V) ->
    ?trace("enc(~9999p, ~9999p)", [_K, _V]),
    <<-1:16/big-signed>>;
enc(string = _K, S) ->
    ?trace("enc(~9999p, ~9999p)", [_K, S]),
    if is_list(S) ->
            [<<(length(S)):16/big-signed>>, S];
       is_binary(S) ->
            <<(size(S)):16/big-signed, S/binary>>
    end;
enc(bytes = _K, undefined = _V) ->
    ?trace("enc(~9999p, ~9999p)", [_K, _V]),
    <<-1:32/big-signed>>;
enc(bytes = _K, B) ->
    ?trace("enc(~9999p, ~9999p)", [_K, B]),
    <<(size(B)):32/big-signed, B/binary>>;
enc({msg, Spec} = _K, B) ->
    ?trace("enc(~9999p, ~9999p)", [_K, B]),
    Encoded = iolist_to_binary(enc(Spec, B)),
    Length = size(Encoded) + 4, %% + size of CRC field
    CRC = erlang:crc32(Encoded),
    %% As defined in the protocol reference, the CRC field is
    %% encoded as signed 32bit integer. It can be true in
    %% some languages like Python which have zlib.crc32()
    %% function which returns signed 32bit integer, but this
    %% is not true in the Erlang: erlang:crc32/1 function
    %% returns unsigned 32bit integer, so we encode it here as
    %% unsigned too.
    <<Length:32/big-signed, CRC:32/big-unsigned, Encoded/binary>>;
enc({mset, Spec} = _K, L) ->
    ?trace("enc(~9999p, ~9999p)", [_K, L]),
    %% Special case for MessageSet array, which is actually encoded
    %% as bytes (without int32 header with array length but with
    %% int32 header with byte length).
    %% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    enc(bytes, iolist_to_binary([enc(Spec, E) || E <- L]));
enc([T] = _K, L) ->
    ?trace("enc(~9999p, ~9999p)", [_K, L]),
    [<<(length(L)):32/big-signed>> | [enc(T, E) || E <- L]];
enc({Types} = _K, Seq = _V) ->
    ?trace("enc(~9999p, ~9999p)", [_K, _V]),
    [enc(T, O) || {T, O} <- lists:zip(Types, tuple_to_list(Seq))].

%% @doc Decode binary data representing encoded Kafka request/response
%% message according to specification.
-spec dec(Spec :: ktype(), Encoded :: binary()) ->
                 {Decoded :: any(), Tail :: binary()}.
dec(int8 = _K, <<I:8/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {I, Tail};
dec(int16 = _K, <<I:16/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {I, Tail};
dec(int32 = _K, <<I:32/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {I, Tail};
dec(int64 = _K, <<I:64/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {I, Tail};
dec(string = _K, <<-1:16/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {undefined, Tail};
dec(string = _K, <<Len:16/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {S, Tail1} = split_binary(Tail, Len),
    {binary_to_list(S), Tail1};
dec(bytes = _K, <<-1:32/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    {undefined, Tail};
dec(bytes = _K, <<Len:32/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    split_binary(Tail, Len);
dec({msg, Spec} = _K, <<Len:32/big-signed, CRC:32/big-unsigned, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    %% As defined in the protocol reference, the CRC field is
    %% encoded as signed 32bit integer. It can be true in
    %% some languages like Python which have zlib.crc32()
    %% function which returns signed 32bit integer, but this
    %% is not true in the Erlang: erlang:crc32/1 function
    %% returns unsigned 32bit integer, so we decode it here as
    %% unsigned too.
    {Encoded, Tail1} = split_binary(Tail, Len - 4), %% - length of CRC field
    CRC = erlang:crc32(Encoded),
    {Decoded, <<>>} = dec(Spec, Encoded),
    {Decoded, Tail1};
dec({mset, Spec} = _K, <<ByteLength:32/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    %% Special case for MessageSet array, which is actually encoded
    %% as bytes (without int32 header with array length but with
    %% int32 header with byte length).
    %% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    {Encoded, Tail1} = split_binary(Tail, ByteLength),
    {dec_mset(Spec, Encoded), Tail1};
dec([Type] = _K, <<Len:32/big-signed, Tail/binary>> = _V) ->
    ?trace("dec(~9999p, ~9999p)", [_K, _V]),
    dec_array(Type, Len, Tail, []);
dec({Types} = _K, Encoded) ->
    ?trace("dec(~9999p, ~9999p)", [_K, Encoded]),
    dec_seq(Types, Encoded, []).

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Helper for the dec/2 function.
%% Decode MessageSet sequence.
%% Documentation states that server is allowed to return a partial
%% message at the end of the message set:
%% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI
%% So we just drop trailing data which cannot be properly decoded.
-spec dec_mset(Spec :: kseq(), Encoded :: binary()) -> list().
dec_mset(Spec, Encoded) ->
    try
        {Elem, Tail} = dec(Spec, Encoded),
        [Elem | dec_mset(Spec, Tail)]
    catch
        _ExcType:_ExcReason ->
            []
    end.

%% @doc Helper for the dec/2 function.
%% Decode array of elements of the same type.
-spec dec_array(ktype(),
                Length :: non_neg_integer(),
                Encoded :: binary(),
                Accum :: list()) ->
                       {Decoded :: list(), Tail :: binary()}.
dec_array(_Type, 0, Tail, Accum) ->
    {lists:reverse(Accum), Tail};
dec_array(Type, Length, Encoded, Accum) ->
    {Elem, Tail} = dec(Type, Encoded),
    dec_array(Type, Length - 1, Tail, [Elem | Accum]).

%% @doc Helper for the dec/2 function.
%% Decode sequence of elements of different types.
-spec dec_seq([ktype()], Encoded :: binary(), Accum :: list()) ->
                     {Decoded :: tuple(), Tail :: binary()}.
dec_seq([], Tail, Accum) ->
    {list_to_tuple(lists:reverse(Accum)), Tail};
dec_seq([Type | Types], Encoded, Accum) ->
    {Elem, Tail} = dec(Type, Encoded),
    dec_seq(Types, Tail, [Elem | Accum]).

%% ----------------------------------------------------------------------
%% Unit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

-define(
   __assert(Type, Value),
   {lists:flatten(
      io_lib:format(
        "Type ~9999p with value ~9999p",
        [Type, Value])),
    ?_assertMatch({Value, <<>>}, dec(Type, iolist_to_binary(enc(Type, Value))))}).

main_test_() ->
    [?__assert(int8, -16#80), ?__assert(int8, 16#7f),
     ?__assert(int16, -16#8000), ?__assert(int16, 16#7fff),
     ?__assert(int32, -16#80000000), ?__assert(int32, 16#7fffffff),
     ?__assert(int64, -16#8000000000000000), ?__assert(int64, 16#7fffffffffffffff),
     ?__assert(string, undefined), ?__assert(string, ""), ?__assert(string, "hello"),
     ?__assert(bytes, undefined), ?__assert(bytes, <<>>), ?__assert(bytes, <<"hello">>),
     ?__assert([int8], []), ?__assert([int8], [0]), ?__assert([int8], [-1, 0, 1]),
     ?__assert([string], [undefined, "hello", "", "world"]),
     ?__assert([bytes], [undefined, <<"hello">>, <<>>, <<"world">>, undefined]),
     ?__assert([[int8]], [[]]),
     ?__assert([[int8]], [[0]]),
     ?__assert({[int8, bytes, int16, string, bytes]},
               {-128, undefined, -512, "hello, ", <<"world">>}),
     ?__assert({mset, int8}, [-1, 0, 1]),
     {"Testing CRC",
      fun() ->
              Spec = {msg, string},
              Value = "hello, world!",
              Encoded = iolist_to_binary(enc(Spec, Value)),
              Changed = binary:replace(Encoded, <<"world">>, <<"werld">>),
              ?assertMatch({Value, <<>>}, dec(Spec, Encoded)),
              ?assertError(_, dec(Spec, Changed))
      end}
    ].

-endif.
