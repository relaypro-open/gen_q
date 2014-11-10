-module(gen_q_port_tests).
-include_lib("eunit/include/eunit.hrl").

-define(EQE(Type, Value), ?assertMatch({ok, {Type, Value}}, gen_q_port:apply(P, H, "(::)", Type, Value))).

e2q2e_test() ->
    {ok, Q} = test_q:start(500),
    {ok, P} = gen_q_port:start([]),
    {ok, H} = gen_q_port:hopen(P, "localhost", 5000, "us:pa", 1000),
    ?EQE(integer, 2),
    test_q:stop(Q, P, H).
    % For some reason, stoppping the port causes rebar to segfault
    %ok = gen_q_port:stop(P),
