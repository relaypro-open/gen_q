all:
	@(./rebar compile)

clean:
	@(rm -f c_src/*.o)
	@(rm -f priv/gen_q.so)

test: all
	@(test/configure)
	@(./rebar eunit skip_deps=true)
