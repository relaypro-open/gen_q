-module(gen_q_port_tests).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    {ok, P} = gen_q_port:start([]),
    ok = gen_q_port:stop(P).
