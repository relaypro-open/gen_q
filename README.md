gen_q
=====
An Erlang port driver for bridging an Erlang app and a q process, implemented with
asyncronous tasks to allow parallel execution. Built on top of kx's C bindings (`c.o`).

Works for Mac OSX and Linux, developed on OS X Mavericks and CentOS 6.4, running on a production kdb+ instance that writes more than 7 GB of data daily.

__gen_q__ implements q's full list of rich data types and provides access to arbitrary
q function calls via IPC. With __gen_q__, you can feed your tickerplant directly
from an Erlang application, and you can run queries to pull data back out again.

Getting started
---------------
Add the following to your Erlang app's `rebar.config`.
```Erlang
{gen_q, ".*", {git, "git@github.com:republicwireless-open/gen_q.git", {tag, "1.0"}}}
```
Before compliging, if your target q process is version 3 or higher, you must edit __gen_q__'s `rebar.config` and add `-DKXVER=3` to the CFLAGS. Then, compile and start your app.

Start the port driver with the following command.
```Erlang
> {ok, _Pid} = q:start_link().
{ok,<0.1401.0>}
```
Open an IPC handle to your q process.
```Erlang
%                   Host         Port  Credentials          Timeout
> {ok, H} = q:hopen("localhost", 5000, "username:password", 10000).
{ok,18}
```
Evaluate a q expression from Erlang.
```Erlang
> q:eval(H, "2+2").
{ok,{long,4}}
```
Notice that results returned from the `q` module include the data type from 
the q result. This gets more interesting with some of q's more esoteric types.
```Erlang
> q:eval(H, ".z.p").
{ok,{timestamp,478041037666903000}}
```
You can also call functions.
```Erlang
> q:apply(H, sum, {list, float}, [1,2,3]).
{ok,{float,6.0}}
```
And close your connection.
```Erlang
> q:hclose(H).
ok
```
See the unit tests in `test/gen_q_port_tests.erl` for more examples.
Building and Dependencies
-------------------------
If you're targeting a q instance version 3 or higher, you must
add `-DKXVER=3` to your `CFLAGS` in __gen_q__'s `rebar.config`.

If you're compiling for __Darwin__ (Mac OS X), there are a couple caveats:
* A 32bit version of `libgcc_s.1` is required. The `rebar.config` includes the path `/opt/local/lib/gcc48` in an attempt to locate the file. Edit as needed.
* On Darwin, __gen_q__ does not compile within the rebar dependency tree. Change directory to `deps/gen_q` and run `make` from there.
