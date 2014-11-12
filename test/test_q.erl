-module(test_q).
-compile([export_all]).
% This is a pretty big kludge - it works within the unit test only!

start() ->
    Pid = self(),
    Port = 5000,
    QPid = spawn(fun() ->
                P5 = erlang:open_port({spawn, "/usr/bin/env QHOME=/Users/jstimpson/dev/q/q /Users/jstimpson/dev/q/q/m32/q /Users/jstimpson/dev/erlang/gen_q/test/test_q.q -p " ++ integer_to_list(Port)},
                        [stderr_to_stdout, in, exit_status,
                            binary, stream, {line, 255}]),
                loop(P5, Pid, list_to_binary(integer_to_list(Port)))
        end),
    receive
        ok ->
            {ok, QPid};
        {error, Desc} ->
            exit(QPid, kill),
            {error, Desc}
    end.

loop(P5, Pid, Ready) ->
    Pid2 = receive
        {P5, {data, {eol, Ready}}} when Pid =/= undefined ->
            io:format("Sending ok ~p~n", [Pid]),
            Pid ! ok, undefined;
        {P5, {data, {eol, <<"'", Rest/binary>>}}} when Pid =/= undefined ->
            Pid ! {error, Rest}, undefined;
        {P5, Data} ->
            io:format("Data ~p~n", [Data]),
            Pid
    end,
    loop(P5, Pid2, Ready).

stop(QPid, _PortPid, QHandle) ->
    exit(QPid, kill),
    q:hkill(QHandle).

