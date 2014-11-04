-module(gen_q_lifecycle).

-export([start/0, stop/0]).

-include("../include/gen_q.hrl").

-define(APP_STOP_TIMEOUT, 10000).
-define(APPS, [kernel, stdlib, lager, inets, ?APP]).

app_string() ->
    atom_to_list(?APP).

start() ->
    lager:start(),
    lager:info("starting " ++ app_string()),
    case start_apps(apps(), []) of
        {ok, _StartedApps} ->
            ok;
        {Error, StartedApps} ->
            ?LOG(error, "Failed to start all applications, ~p", [Error]),
            stop(StartedApps),
            ok
    end.

stop() ->
    stop(apps()).

apps() ->
    ?APPS.

stop(Apps) ->
    case do_node_stop(Apps) of
        ok ->
            ok;
        Error ->
            ?LOG(error, "Not halting vm due to error, ~p", [Error]),
            ok
    end.

start_apps([], Started) ->
    ?LOG(info, "Started all applications", []),
    {ok, lists:reverse(Started)};
start_apps([A|Apps], Started) ->
    ?LOG(info, "Starting ~p...", [A]),
    case ensure_started(A) of
        ok ->
            ?LOG(info, "Started ~p", [A]),
            start_apps(Apps, [A|Started]);
        Error ->
            ?LOG(error, "Unable to start ~p, ~p", [A, Error]),
            {Error, lists:reverse(Started)}
    end.

stop_apps([]) ->
    ?LOG(info, "Stopped all applications", []),
    ok;
stop_apps([A|Apps]) ->
    ?LOG(info, "Stopping ~p...", [A]),
    case application:stop(A) of
        ok ->
            stop_apps(Apps);
        Error ->
            ?LOG(error, "Unable to stop ~p, ~p", [A, Error]),
            Error
    end.

do_node_stop(Apps) ->
    Node = safe_list_to_atom(app_string() ++ node_str()),
    case node() of
        Node ->
            case stop_apps(lists:reverse(Apps)) of
                ok ->
                    finish_vm(),
                    ok;
                Error ->
                    ?LOG(error, "Failed to stop all applications, ~p", [Error]),
                    Error
            end;
        _ ->
            rpc:call(Node, ?MODULE, stop, []),
            halt()
    end.

node_str() ->
    Node = atom_to_list(node()),
    string:substr(Node, string:rchr(Node, $@)).

finish_vm() ->
    case application:which_applications(?APP_STOP_TIMEOUT) of
        [] ->
            ok;
        A ->
            ?LOG(error, "Applications still running ~p", [A]),
            ok
    end,
    ?LOG(info, "Halting vm", []),
    halt().

ensure_started(App) ->
    case lists:keyfind(App, 1, application:which_applications()) of
        false ->
            case application:start(App) of
                ok ->
                    ok;
                {error, {already_started, App}} ->
                    ok;
                Error ->
                    Error
            end;
        _ -> 
            ok
    end.

safe_list_to_atom(Value) when is_atom(Value) ->
    Value;
safe_list_to_atom(Value) when is_list(Value) ->
    try
        list_to_existing_atom(Value)
    catch 
        error:badarg ->
            erlang:list_to_atom(Value)
    end.
