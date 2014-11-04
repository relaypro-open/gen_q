-module(gen_q).
-export([start/0, stop/0, priv_dir/0]).

-include("../include/gen_q.hrl").

start() ->
    gen_q_lifecycle:start().

stop() ->
    gen_q_lifecycle:stop().

priv_dir() ->
    filename:join(root_dir(), "priv").

root_dir() ->
    {file, File} = code:is_loaded(?APP),
    filename:dirname(filename:dirname(File)).
