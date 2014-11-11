-module(gen_q_port).

-export([hopen/5, apply/5, dot/5, eval/3, hclose/2, hkill/2, decode_binary/2]).

-export([start/1, start_link/1, stop/1, init/1, call_port/2]).

-define(SharedLib, "gen_q_drv").

-define(FuncOpts, 0).
-define(FuncQHOpen, 1).
-define(FuncQHClose, 2).
-define(FuncQApply, 3).
-define(FuncQHKill, 4).
-define(FuncQDecodeBinary, 5).

-include("../include/gen_q.hrl").

hopen(Pid, Host, Port, UserPass, Timeout) ->
    call_port(Pid, {?FuncQHOpen, [Host, Port, UserPass, Timeout]}).

apply(Pid, Handle, Func, Types, Values) when is_atom(Func) ->
    apply(Pid, Handle, atom_to_list(Func), Types, Values);
apply(Pid, Handle, Func, Types, Values) ->
    case call_port(Pid, {?FuncQApply, [Handle, Func, {Types, Values}]}) of
        {ok, {ok, ok}} -> % (::) and function projections
            ok;
        Result ->
            Result
    end.

eval(Pid, Handle, Expr) ->
    apply(Pid, Handle, Expr, ok, ok).

dot(Pid, Handle, Function, ArgTypes, ArgVals) when is_atom(Function) ->
    apply(Pid, Handle, "{.[x 0;x 1]}",
        {list, [symbol, ArgTypes]},
        [Function, ArgVals]).

hclose(Pid, Handle) ->
    call_port(Pid, {?FuncQHClose, [Handle]}).

hkill(Pid, Handle) ->
    call_port(Pid, {?FuncQHKill, [Handle]}).

decode_binary(Pid, Binary) ->
    call_port(Pid, {?FuncQDecodeBinary, [Binary]}).

call_port(Pid, Msg) ->
    Pid ! {call, self(), Msg},
    receive
        Result ->
            Result
    end.

start(Opts) ->
    case load_driver() of
        ok ->
            case spawn(?MODULE, init, [Opts]) of
                {error, Error} ->
                    {error, Error};
                Pid ->
                    {ok, Pid}
            end;
        {error, Error} ->
            {error, Error}
    end.

start_link(Opts) ->
    case load_driver() of
        ok ->
            case spawn_link(?MODULE, init, [Opts]) of
                {error, Error} ->
                    {error, Error};
                Pid ->
                    {ok, Pid}
            end;
        {error, Error} ->
            {error, Error}
    end.

stop(Pid) ->
    Pid ! stop,
    ok.

load_driver() ->
    Dir = gen_q:priv_dir(),
    case erl_ddll:load(Dir, ?SharedLib) of
        ok ->
            ok;
        {error, Error} ->
            exit({error, erl_ddll:format_error(Error)})
    end.

init(Opts) ->
    Port = open_port({spawn, ?SharedLib}, [binary]),
    send_async_port_command(Port, {?FuncOpts, Opts}),
    receive
        {Port, {data, Data}} ->
            ok = binary_to_term(Data)
    end,
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
