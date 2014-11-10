-module(gen_q_port_tests).
-include_lib("eunit/include/eunit.hrl").

-define(_EQE(Type, Value), ?_assertMatch({ok, {Type, Value}}, gen_q_port:apply(P, H, "(::)", Type, Value))).

-record(ctx, {qpid, port, h}).

gen_q_port_test_() ->
    {setup,
            fun setup/0,
            fun teardown/1,
            fun(#ctx{port=P, h=H}) ->
                    [?_EQE(integer, 2)]
            end}.

setup() ->
    {ok, Q} = test_q:start(500),
    {ok, P} = gen_q_port:start([]),
    {ok, H} = gen_q_port:hopen(P, "localhost", 5000, "us:pa", 1000),
    #ctx{qpid=Q, port=P, h=H}.

teardown(#ctx{qpid=Q, port=P, h=H}) ->
    test_q:stop(Q, P, H).
    % For some reason, stoppping the port causes rebar to segfault
    %ok = gen_q_port:stop(P),
