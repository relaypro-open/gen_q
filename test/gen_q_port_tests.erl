-module(gen_q_port_tests).
-include_lib("eunit/include/eunit.hrl").
-include("gen_q_test.hrl").

-define(_EQE(Type, Value), ?_assertMatch({ok, {Type, Value}}, gen_q_port:apply(P, H, "(::)", Type, Value))).
-define(_EQE2(Type, InValue, OutValue), ?_assertMatch({ok, {Type, OutValue}}, gen_q_port:apply(P, H, "(::)", Type, InValue))).
-define(_EQE3(InType, OutType, Value), ?_assertMatch({ok, {OutType, Value}}, gen_q_port:apply(P, H, "(::)", InType, Value))).

-define(EQE_NEIGHBOR(Type, Value, Eps),
          begin
                  ((fun (__ExpectType, __ExpectValue) ->
                      case gen_q_port:apply(P, H, "(::)", Type, Value) of
                          {ok, {__ExpectType, __Value2}} ->
                              ?assertNeighborhood(__ExpectValue, __Value2, Eps);
                          {ok, {__Type2, _}} ->
                              erlang:error({'EQE_NEIGHBOOR_type_failed',
                                      [{module, ?MODULE},
                                          {line, ?LINE},
                                          {expression, {(??Type), (??Value)}},
                                          {expected, __ExpectType},
                                          {value, __Type2}]});
                          __OtherResult ->
                              erlang:error({'EQE_NEIGHBOOR_failed',
                                      [{module, ?MODULE},
                                          {line, ?LINE},
                                          {expression, {(??Type), (??Value)}},
                                          {result, __OtherResult}]})
                      end
              end)(Type, Value))
    end).
-define(_EQE_NEIGHBOOR(Type, Value, Eps), ?_test(?EQE_NEIGHBOR(Type, Value, Eps))).

-record(ctx, {qpid, port, h}).

gen_q_port_test_() ->
    {setup,
            fun setup/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
                    [?_EQE(time, 43200000), % `int$12t
                     ?_EQE(second, 45042), % `int$`second$12:30:42
                     ?_EQE(minute, 750), % `int$`minute$12:30:42
                     ?_EQE(timespan, 19647590000), % `long$2014.11.10D16:27:40.805091000-2014.11.10D16:27:21.157501000
                     ?_EQE(datetime, 5427.693), % `float$2014.11.10T16:38:04.679
                     ?_EQE(date, 5427), % `int$2014.11.10
                     ?_EQE(month, 178), % `int$2014.11m
                     ?_EQE(integer, 2),
                     ?_EQE(timestamp, 468953337121125000), % `long$2014.11.10D16:48:57.121125000
                     ?_EQE(long, 2),
                     ?_EQE(float, 0.456),
                     ?_EQE(char, $A),
                     ?_EQE_NEIGHBOOR(real, 0.1, 0.0001),
                     ?_EQE(short, 3),
                     ?_EQE(byte, 255),
                     ?_EQE(boolean, 1),
                     ?_EQE(symbol, 'hello'),
                     ?_EQE(string, "hello"),
                     ?_EQE2(string, <<"hello">>, "hello"),
                     ?_EQE({list, symbol}, [abc, def]),
                     ?_EQE({list, integer}, [1,2,3]),
                     ?_EQE({list, long}, [10,11,12]),
                     ?_EQE({list, byte}, [5,6,7]),
                     ?_EQE({list, short}, [15,16,17]),
                     ?_EQE({list, real}, [1.1, 2.2, 3.3]),
                     ?_EQE({list, float}, [5.5, 6.5, 7.5])
                 ] ++

                 % Test erlang encoding lists of small ints as strings:
                 [ ?_EQE({list, X}, [1,2,3]) || X <-
                     [time, second, minute, date, month,
                         timespan, timestamp, byte, boolean,
                         short, datetime, float, real, integer, long] ] ++
                 [ ?_EQE3({list, char}, string, [1,2,3])] ++

                 % Test erlang encoding lists of big ints as lists:
                 [ ?_EQE({list, X}, [1000,2000,3000]) || X <-
                     [time, second, minute, date, month,
                         timespan, timestamp, byte, boolean,
                         short, datetime, float, real, integer, long] ]
            end}.

gen_q_port_utiqd_test_() ->
    {setup,
            fun setup_utiqd/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
                    [?_EQE(datetime, 1415630000),
                     ?_EQE({list, datetime}, [1,2,3]),
                     ?_EQE({list, datetime}, [1000,2000,3000])]
            end}.

gen_q_port_dsiqt_test_() ->
    {setup,
            fun setup_dsiqt/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
                    [?_EQE(time, 43200),
                     ?_EQE({list, time}, [1,2,3]),
                     ?_EQE({list, time}, [1000,2000,3000])]
            end}.

setup() ->
    setup([]).

setup_utiqd() ->
    setup([unix_timestamp_is_q_datetime]).

setup_dsiqt() ->
    setup([day_seconds_is_q_time]).

setup(Opts) ->
    {ok, Q} = test_q:start(),
    {ok, P} = gen_q_port:start(Opts),
    {ok, H} = gen_q_port:hopen(P, "localhost", 5000, "us:pa", 1000),
    #ctx{qpid=Q, port=P, h=H}.

teardown(#ctx{qpid=Q, port=P, h=H}) ->
    test_q:stop(Q, P, H).
    % For some reason, stoppping the port causes rebar to segfault
    %ok = gen_q_port:stop(P),
