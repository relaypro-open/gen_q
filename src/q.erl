-module(q).
-behavior(gen_server).

%% External exports
-export([start/0, start/1, start/2, start_link/0, start_link/1, start_link/2, stop/0, stop/1]).

%% API functions
-export([hopen/4, hopen/5,
         hclose/1, hclose/2,
         hkill/1, hkill/2,
         decode_binary/1, decode_binary/2,
         apply/4, apply/5, apply/6,
         dot/4, dot/5, dot/6,
         eval/2, eval/3, eval/4]).

-export([state/0, state/1]).

%% gen_server callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        code_change/3,
        terminate/2]).

-define(SharedLib, "gen_q_drv").

-define(FuncOpts, 0).
-define(FuncQHOpen, 1).
-define(FuncQHClose, 2).
-define(FuncQApply, 3).
-define(FuncQHKill, 4).
-define(FuncQDecodeBinary, 5).

-define(PortTimeout, 600000).

-include("../include/gen_q.hrl").

-record(state, {port, port_timeout}).

start() ->
    start([]).

start(Options) ->
    start({local, ?MODULE}, Options).

start(ServerName, Options) ->
    case load_driver() of
        ok ->
            gen_server:start(ServerName, ?MODULE, Options, []);
        {error, Error} ->
            {error, Error}
    end.

stop() ->
    stop(?MODULE).

stop(SvrRef) ->
    gen_server:call(SvrRef, {stop}, infinity).

state() ->
    state(?MODULE).

state(SvrRef) ->
    gen_server:call(SvrRef, {get_state}, infinity).

start_link() ->
    start_link([]).

start_link(Options) ->
    start_link({local, ?MODULE}, Options).

start_link(ServerName, Options) ->
    case load_driver() of
        ok ->
            gen_server:start_link(ServerName, ?MODULE, Options, []);
        {error, Error} ->
            {error, Error}
    end.

priv_dir() ->
    filename:join(root_dir(), "priv").

root_dir() ->
    {file, File} = code:is_loaded(q),
    filename:dirname(filename:dirname(File)).

load_driver() ->
    Dir = priv_dir(),
    case erl_ddll:load(Dir, ?SharedLib) of
        ok ->
            ok;
        {error, Error} ->
            exit({error, erl_ddll:format_error(Error)})
    end.

hopen(Host, Port, Userpass, Timeout) ->
    hopen(?MODULE, Host, Port, Userpass, Timeout).

hopen(SvrRef, Host, Port, Userpass, Timeout) ->
    gen_server:call(SvrRef, {hopen, Host, Port, Userpass, Timeout}, infinity).

hclose(Handle) ->
    hclose(?MODULE, Handle).

hclose(SvrRef, Handle) ->
    gen_server:call(SvrRef, {hclose, Handle}, infinity).

hkill(Handle) ->
    hkill(?MODULE, Handle).

hkill(SvrRef, Handle) ->
    gen_server:call(SvrRef, {hkill, Handle}, infinity).

decode_binary(Binary) ->
    decode_binary(?MODULE, Binary).

decode_binary(SvrRef, Binary) ->
    gen_server:call(SvrRef, {decode_binary, Binary}, infinity).

apply(Handle, Func, Types, Values) when is_atom(Func) ->
    apply(Handle, atom_to_list(Func), Types, Values);
apply(Handle, Func, Types, Values) ->
    apply(?MODULE, Handle, Func, Types, Values, infinity).

apply(SvrRef, Handle, Func, Types, Values) when is_atom(Func) ->
    apply(SvrRef, Handle, atom_to_list(Func), Types, Values);
apply(SvrRef, Handle, Func, Types, Values) ->
    apply(SvrRef, Handle, Func, Types, Values, infinity).

apply(SvrRef, Handle, Func, Types, Values, Timeout) when is_atom(Func) ->
    apply(SvrRef, Handle, atom_to_list(Func), Types, Values, Timeout);
apply(SvrRef, Handle, Func, Types, Values, Timeout) ->
    case gen_server:call(SvrRef, {apply, Handle, Func, Types, Values}, Timeout) of
        {ok, {ok, ok}} -> %% (::) and function projections
            ok;
        Result ->
            Result
    end.

dot(Handle, Func, ArgTypes, ArgVals) when is_atom(Func) ->
    dot(?MODULE, Handle, Func, ArgTypes, ArgVals, infinity).

dot(SvrRef, Handle, Func, ArgTypes, ArgVals) when is_atom(Func) ->
    dot(SvrRef, Handle, Func, ArgTypes, ArgVals, infinity).

dot(SvrRef, Handle, Func, ArgTypes, ArgVals, Timeout) when is_atom(Func) ->
    apply(SvrRef, Handle, '{.[x 0;x 1]}', {list, [symbol, ArgTypes]},
        [Func, ArgVals], Timeout).

eval(Handle, Expr) ->
    eval(?MODULE, Handle, Expr).

eval(SvrRef, Handle, Expr) ->
    eval(SvrRef, Handle, Expr, infinity).

eval(SvrRef, Handle, Expr, Timeout) ->
    apply(SvrRef, Handle, Expr, ok, ok, Timeout).

init(Opts) ->
    process_flag(trap_exit, true),
    Port = open_port({spawn, ?SharedLib}, [binary]),
    PortTimeout = proplists:get_value(port_timeout, Opts, ?PortTimeout),
    Opts2 = lists:filter(fun(unix_timestamp_is_q_datetime) -> true;
            (day_seconds_is_q_time) -> true;
            (_) -> false
        end, Opts),
    send_async_port_command(Port, {?FuncOpts, Opts2}),
    case recv_async_port_result(Port, PortTimeout) of
        {reply, ok} ->
            {ok, #state{port=Port,
                    port_timeout=PortTimeout}};
        {stop, port_timeout} ->
            {stop, port_timeout}
    end.

handle_call({hopen, Host, Port, Userpass, Timeout}, _From, State) ->
    do_call(State, {?FuncQHOpen, [Host, Port, Userpass, Timeout]});
handle_call({hclose, Handle}, _From, State) ->
    do_call(State, {?FuncQHClose, [Handle]});
handle_call({hkill, Handle}, _From, State) ->
    do_call(State, {?FuncQHKill, [Handle]});
handle_call({decode_binary, Binary}, _From, State) ->
    do_call(State, {?FuncQDecodeBinary, [Binary]});
handle_call({apply, Handle, Func, Types, Values}, _From, State) ->
    do_call(State, {?FuncQApply, [Handle, Func, {Types, Values}]});
handle_call({get_state}, _From, State) ->
    {reply, State, State};
handle_call({stop}, _From, State) ->
    {stop, normal, State}.

do_call(State=#state{port=Port, port_timeout=PortTimeout}, Msg) ->
    send_async_port_command(Port, Msg),
    case recv_async_port_result(Port, PortTimeout) of
        {reply, Reply} ->
            {reply, Reply, State};
        {stop, port_timeout} ->
            {stop, port_timeout, State}
    end.

send_async_port_command(Port, Msg) ->
    % Note: erlang:port_command is a synchronous call
    Port ! {self(), {command, term_to_binary(Msg)}}.

recv_async_port_result(Port, PortTimeout) ->
    receive
        {Port, {data, Data}} ->
            {reply, binary_to_term(Data)}
    after
        PortTimeout ->
            {stop, port_timeout}
    end.

handle_info({'EXIT', Port, Reason}, State=#state{port=Port}) ->
    {stop, {port_terminated, Reason}, State};
handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, {server_terminated, Reason}, State}.

terminate({port_terminated, _Reason}, _State) ->
    ok;
terminate(_Reason, _State=#state{port=Port}) ->
    port_close(Port).

handle_cast(_Msg, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
