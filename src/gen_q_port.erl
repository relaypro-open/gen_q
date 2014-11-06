-module(gen_q_port).

-export([hopen/5, apply/5, hclose/2]).

-export([start/0, start_link/0, stop/1, init/0, call_port/2]).

-export([test_async/0, test_apply/1]).

-define(SharedLib, "gen_q_drv").
-define(FuncQHOpen, 1).
-define(FuncQHClose, 2).
-define(FuncQApply, 3).

-include("../include/gen_q.hrl").

hopen(Pid, Host, Port, UserPass, Timeout) ->
    call_port(Pid, {?FuncQHOpen, [Host, Port, UserPass, Timeout]}).

apply(Pid, Handle, Func, Types, Values) ->
    call_port(Pid, {?FuncQApply, [Handle, Func, {Types, Values}]}).

hclose(Pid, Handle) ->
    call_port(Pid, {?FuncQHClose, [Handle]}).

test_async() ->
    {ok, Q1} = gen_q_port:start(),
    {ok, Q2} = gen_q_port:start(),
    Tick = now(),
    spawn(fun() ->
                R1 = gen_q_port:call_port(Q1, 2000),
                io:format("done 1 ~p, ~p~n", [R1, timer:now_diff(now(), Tick)]),
                stop(Q1)
        end),
    spawn(fun() ->
                R2 = gen_q_port:call_port(Q2, 3000),
                io:format("done 2 ~p, ~p~n", [R2, timer:now_diff(now(), Tick)]),
                stop(Q2)
        end).

test_apply(Func) ->
    {ok, P} = start(),
    {ok, H} = hopen(P, "localhost", 5000, "us:pa", 1000),
    R = apply(P, H, Func, string, "Test"),
    ok = hclose(P, H),
    stop(P),
    R.

call_port(Pid, Msg) ->
    Pid ! {call, self(), Msg},
    receive
        Result ->
            Result
    end.

start() ->
    case load_driver() of
        ok ->
            case spawn(?MODULE, init, []) of
                {error, Error} ->
                    {error, Error};
                Pid ->
                    {ok, Pid}
            end;
        {error, Error} ->
            {error, Error}
    end.

start_link() ->
    case load_driver() of
        ok ->
            case spawn_link(?MODULE, init, []) of
                {error, Error} ->
                    {error, Error};
                Pid ->
                    {ok, Pid}
            end;
        {error, Error} ->
            {error, Error}
    end.

stop(Pid) ->
    Pid ! stop.

load_driver() ->
    Dir = gen_q:priv_dir(),
    case erl_ddll:load(Dir, ?SharedLib) of
        ok ->
            ok;
        {error, Error} ->
            exit({error, erl_ddll:format_error(Error)})
    end.

init() ->
    Port = open_port({spawn, ?SharedLib}, [binary]),
    loop(Port).

loop(Port) ->
    receive
        {call, Caller, Msg} ->
            send_async_port_command(Port, Msg),
            recv_async_port_result(Port, Caller),
            loop(Port);
        stop ->
            send_async_port_close(Port),
            recv_async_port_close_result(Port);
        {'EXIT', Port, Reason} ->
            ?LOG(info, "gen_q_port exiting ~p", [Reason]),
            exit(port_terminated)
    end.

send_async_port_command(Port, Msg) ->
    % Note: erlang:port_command is a synchronous call
    Port ! {self(), {command, term_to_binary(Msg)}}.

recv_async_port_result(Port, Caller) ->
    receive
        {Port, {data, Data}} ->
            Caller ! binary_to_term(Data)
    end.

send_async_port_close(Port) ->
    Port ! {self(), close}.

recv_async_port_close_result(Port) ->
    receive
        {Port, closed} ->
            exit(normal)
    end.
