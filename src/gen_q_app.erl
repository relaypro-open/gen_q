-module(gen_q_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-include("../include/gen_q.hrl").

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    write_pid_file(pid_file()),
    gen_q_sup:start_link().

stop(_State) ->
    ok.

write_pid_file(FileName) ->
    Pid = os:getpid(),
    {ok, File} = file:open(FileName, [write]),
    file:write(File, Pid),
    file:close(File).

pid_file() ->
    case application:get_env(?APP, pid_file) of
        {ok, File} -> File;
        _ -> "/var/run/"++atom_to_list(?APP)++"/"++atom_to_list(?APP)++".pid"
    end.
