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
Start the port driver with the following command. (We recommend using a supervisor for
anything beyond a toy implementation.)
```Erlang
q:start_link().
```
Open an IPC handle to your q process.
```Erlang
%       Host         Port  Credentials          Timeout
q:hopen("localhost", 5000, "username:password", 10000).
```

Building
--------
If you're targeting a q instance version 3 or higher, you must
add `-DKXVER=3` to your `CFLAGS` in `rebar.config`

Development
-----------

Dependencies
------------
Darwin
GCC 4.8: i386 libgcc_s.1
