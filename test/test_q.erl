-module(test_q).
-compile([export_all]).

start(Sleep) ->
    Pid = self(),
    QPid = spawn(fun() ->
                Pid ! ok,
                os:cmd("/Users/jstimpson/dev/q/q/q.sh -p 5000")
        end),
    receive
        ok ->
            timer:sleep(Sleep)
    end,
    {ok, QPid}.

stop(QPid, PortPid, QHandle) ->
    exit(QPid, kill),
    gen_q_port:hkill(PortPid, QHandle).

