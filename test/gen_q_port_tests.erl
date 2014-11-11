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
            fun(#ctx{port=P, h=H}) ->
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
                     ?_eqe({list, symbol}, [abc, def]),
                     ?_eqe({list, integer}, [1,2,3]),
                     ?_eqe({list, long}, [10,11,12]),
                     ?_eqe({list, byte}, [5,6,7]),
                     ?_eqe({list, short}, [15,16,17]),
                     ?_eqe_all_neighbor({list, real}, [1.1, 2.2, 3.3], ?realEps),
                     ?_eqe_all_neighbor({list, real}, [5.5, 6.5, 7.5], ?floatEps),

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
                     ?_eqe({list, [string, timestamp]}, ["test", 1236128361])
                 ]
            end}.

gen_q_port_utiqd_test_() ->
    {setup,
            fun setup_utiqd/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
                    [?_eqe(datetime, 1415630000),
                     ?_eqe_all_neighbor({list, datetime}, [1,2,3], 1.5),
                     ?_eqe_all_neighbor({list, datetime}, [1000,2000,3000], 1.5)]
            end}.

gen_q_port_dsiqt_test_() ->
    {setup,
            fun setup_dsiqt/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
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
    {ok, P} = gen_q_port:start(Opts),
    {ok, H} = gen_q_port:hopen(P, "localhost", 5000, "us:pa", 1000),
    #ctx{qpid=Q, port=P, h=H}.

teardown(#ctx{qpid=Q, port=P, h=H}) ->
    test_q:stop(Q, P, H).
    % For some reason, stoppping the port causes rebar to segfault
    %ok = gen_q_port:stop(P),
