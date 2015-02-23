-module(test_q).
-export([start/0, stop/3]).

config(App) ->
    {ok, P} = file:consult(App ++ "/test/test.config"),
    P.

start() ->
    App = filename:dirname(filename:dirname(code:which(?MODULE))),
    Pid = self(),
    Port = 5000,
    Config = config(App),
    QHome = proplists:get_value('QHOME', Config),
    QExec = proplists:get_value('Q', Config),
    QPid = spawn(fun() ->
                P5 = erlang:open_port({spawn, "/usr/bin/env QHOME=\""++QHome++
                        "\" \""++QExec++"\" \""++App++"/test/test_q.q\" -p " ++ 
                        integer_to_list(Port)},

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

