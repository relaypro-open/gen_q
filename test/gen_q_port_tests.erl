-module(gen_q_port_tests).
-include_lib("eunit/include/eunit.hrl").
-include("gen_q_test.hrl").

-define(realEps, 0.0001).
-define(floatEps, 0.000001).

-record(ctx, {qpid, port, h}).

gen_q_port_test_() ->
    {setup,
            fun setup/0,
            fun teardown/1,
            fun(#ctx{h=H}) ->
                    [?_eqe(time, 43200000), % `int$12t
                     ?_eqe(second, 45042), % `int$`second$12:30:42
                     ?_eqe(minute, 750), % `int$`minute$12:30:42
                     ?_eqe(timespan, 19647590000), % `long$2014.11.10D16:27:40.805091000-2014.11.10D16:27:21.157501000
                     ?_eqe_neighbor(datetime, 5427.693, ?floatEps), % `float$2014.11.10T16:38:04.679
                     ?_eqe(date, 5427), % `int$2014.11.10
                     ?_eqe(month, 178), % `int$2014.11m
                     ?_eqe(integer, 2),
                     ?_eqe(timestamp, 468953337121125000), % `long$2014.11.10D16:48:57.121125000
                     ?_eqe(long, 2),
                     ?_eqe_neighbor(float, 0.456, ?floatEps),
                     ?_eqe(char, $A),
                     ?_eqe_neighbor(real, 0.1, ?realEps),
                     ?_eqe(short, 3),
                     ?_eqe(byte, 255),
                     ?_eqe(boolean, 1),
                     ?_eqe(symbol, 'hello'),
                     ?_eqe(string, "hello"),
                     ?_eqe_value(string, <<"hello">>, "hello"),
                     ?_eqe({list, string}, ["first string", "second string"]),
                     ?_eqe({list, symbol}, [abc, def]),
                     ?_eqe({list, integer}, [1,2,3]),
                     ?_eqe({list, long}, [10,11,12]),
                     ?_eqe({list, byte}, [5,6,7]),
                     ?_eqe({list, short}, [15,16,17]),
                     ?_eqe_all_neighbor({list, real}, [1.1, 2.2, 3.3], ?realEps),
                     ?_eqe_all_neighbor({list, real}, [5.5, 6.5, 7.5], ?floatEps),
                     ?_eqe({list, integer}, []),

                     % Test erlang encoding lists of small ints as strings:
                     ?_eqe({list, time}, [1,2,3]),
                     ?_eqe({list, second}, [1,2,3]),
                     ?_eqe({list, minute}, [1,2,3]),
                     ?_eqe({list, date}, [1,2,3]),
                     ?_eqe({list, month}, [1,2,3]),
                     ?_eqe({list, timespan}, [1,2,3]),
                     ?_eqe({list, timestamp}, [1,2,3]),
                     ?_eqe({list, byte}, [1,2,3]),
                     ?_eqe({list, boolean}, [1,2,3]),
                     ?_eqe({list, short}, [1,2,3]),
                     ?_eqe_all_neighbor({list, datetime}, [1,2,3], ?floatEps),
                     ?_eqe_all_neighbor({list, float}, [1,2,3], ?floatEps),
                     ?_eqe_all_neighbor({list, real}, [1,2,3], ?realEps),
                     ?_eqe({list, integer}, [1,2,3]),
                     ?_eqe({list, long}, [1,2,3]),

                     % Test erlang encoding lists of big ints (>255) as lists:
                     ?_eqe({list, time}, [1000,2000,3000]),
                     ?_eqe({list, second}, [1000,2000,3000]),
                     ?_eqe({list, minute}, [1000,2000,3000]),
                     ?_eqe({list, date}, [1000,2000,3000]),
                     ?_eqe({list, month}, [1000,2000,3000]),
                     ?_eqe({list, timespan}, [1000,2000,3000]),
                     ?_eqe({list, timestamp}, [1000,2000,3000]),
                     ?_eqe({list, short}, [1000,2000,3000]),
                     ?_eqe_all_neighbor({list, datetime}, [1000,2000,3000], ?floatEps),
                     ?_eqe_all_neighbor({list, float}, [1000,2000,3000], ?floatEps),
                     ?_eqe_all_neighbor({list, real}, [1000,2000,3000], ?realEps),
                     ?_eqe({list, integer}, [1000,2000,3000]),
                     ?_eqe({list, long}, [1000,2000,3000]),

                     % Mixed lists
                     ?_eqe_type({list, [integer, integer]}, {list, integer}, [1,2]),
                     ?_eqe({list, [integer, long]}, [1, 2]),
                     ?_eqe({list, [integer, long]}, [1, 2000]),
                     ?_eqe({list, [string, timestamp]}, ["test", 1236128361]),
                     ?_eqe({list, [string, string, integer]}, ["hello", "world", 5]),
                     ?_eqe({list, []}, []),
                     ?_eqe({list, [{list, []}]}, [[]]),
                     ?_eqe({list, [{list, []}, {list, []}]}, [[], []]),
                     ?_eqe({list, [{list, integer}, {list, string}]}, [[1, 2], ["deep", "list"]]),
                     ?_eqe({list, [{list, integer}, {list, string}]}, [[1000, 2000], ["deep", "list"]]),
                     ?_eqe({list, [{list, [long, short]}]}, [[5,4]]),

                     % Tables
                     ?_eqe({table, {list, [{list, []},
                                           {list, []}]}}, {[column1, column2], [[], []]}),
                     ?_eqe({table, {list, [{list, []},
                                           {list, byte}]}}, {[column1, column2], [[], []]}),
                     ?_eqe({table, {list, [{list, string},
                                           {list, long},
                                           {list, datetime}]}}, {[string, long, datetime],
                                                                 [["a","b","c","d"],
                                                                  [123123123,112431245435,123412352345,1247124],
                                                                  [342.123, 234.1823, 13412.1238, 12312.12381]]}),

                     % Dicts
                     ?_eqe({dict, {list, integer}, {list, integer}}, {[1,2,3], [4,5,6]}),
                     ?_eqe({dict, {list, symbol}, {list, string}}, {[a,b,c], ["string4","string5","string6"]}),

                     % Keyed table
                     ?_eqe({dict, {table, {list, [{list, symbol}]}},
                                  {table, {list, [{list, long},{list,long}]}}},
                              {{[c],[[a,b,c]]},{[a,b],[[1,2,3],[4,5,6]]}}),

                     % Function definition and execution
                     ?_assertMatch(ok, q:eval(H, "gen_q_port_monad:{`long$x*2}")),
                     ?_assertMatch({ok,{long,10}}, q:apply(H, gen_q_port_monad, long, 5)),
                     ?_assertMatch(ok, q:eval(H, "gen_q_port_diad:{`long$x*y}")),
                     ?_assertMatch({ok,{long,56}}, q:dot(H, gen_q_port_diad, {list, long}, [8, 7])),

                     % Binary decoding
                     ?_assertMatch({ok,{long,1}}, q:decode_binary(
                             <<16#01, 16#00, 16#00, 16#00,
                               16#11, 16#00, 16#00, 16#00,
                               16#f9,
                               16#01, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00>>)),

                     % Nulls and infinitys
                     ?_eqe(time, null),
                     ?_eqe(second, null),
                     ?_eqe(minute, null),
                     ?_eqe(timespan, null),
                     ?_eqe(datetime, null),
                     ?_eqe(date, null),
                     ?_eqe(month, null),
                     ?_eqe(integer, null),
                     ?_eqe(timestamp, null),
                     ?_eqe(long, null),
                     ?_eqe(float, null),
                     ?_eqe(short, null),
                     ?_eqe(time, infinity),
                     ?_eqe(second, infinity),
                     ?_eqe(minute, infinity),
                     ?_eqe(timespan, infinity),
                     ?_eqe(datetime, infinity),
                     ?_eqe(date, infinity),
                     ?_eqe(month, infinity),
                     ?_eqe(integer, infinity),
                     ?_eqe(timestamp, infinity),
                     ?_eqe(long, infinity),
                     ?_eqe(float, infinity),
                     ?_eqe(short, infinity),
                     ?_eqe({list, time}, [null, infinity]),
                     ?_eqe({list, second}, [null, infinity]),
                     ?_eqe({list, minute}, [null, infinity]),
                     ?_eqe({list, timespan}, [null, infinity]),
                     ?_eqe({list, datetime}, [null, infinity]),
                     ?_eqe({list, date}, [null, infinity]),
                     ?_eqe({list, month}, [null, infinity]),
                     ?_eqe({list, integer}, [null, infinity]),
                     ?_eqe({list, timestamp}, [null, infinity]),
                     ?_eqe({list, long}, [null, infinity]),
                     ?_eqe({list, float}, [null, infinity]),
                     ?_eqe({list, short}, [null, infinity])
                 ]
            end}.

gen_q_port_utiqd_test_() ->
    {setup,
            fun setup_utiqd/0,
            fun teardown/1,
            fun(#ctx{h=H}) ->
                    [?_eqe(datetime, 1415630000),
                     ?_eqe_all_neighbor({list, datetime}, [1,2,3], 1.5),
                     ?_eqe_all_neighbor({list, datetime}, [1000,2000,3000], 1.5)]
            end}.

gen_q_port_dsiqt_test_() ->
    {setup,
            fun setup_dsiqt/0,
            fun teardown/1,
            fun(#ctx{h=H}) ->
                    [?_eqe(time, 43200),
                     ?_eqe({list, time}, [1,2,3]),
                     ?_eqe({list, time}, [1000,2000,3000])]
            end}.

setup() ->
    setup([]).

setup_utiqd() ->
    setup([unix_timestamp_is_q_datetime]).

setup_dsiqt() ->
    setup([day_seconds_is_q_time]).

setup(Opts) ->
    {ok, Q} = test_q:start(),
    {ok, P} = q:start(Opts),
    {ok, H} = q:hopen("localhost", 5000, "us:pa", 1000),
    #ctx{qpid=Q, port=P, h=H}.

teardown(#ctx{qpid=Q, port=P, h=H}) ->
    test_q:stop(Q, P, H),
    %% Note: Stopping the port causes *rebar* to segfault for some reason
    catch q:stop().
