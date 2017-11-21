all:
	@(rebar3 compile)

clean:
	@(rm -f c_src/*.o)
	@(rm -f priv/gen_q_drv.so)
	@(rebar3 clean)

test: all
	@(test/configure)
	@(rebar3 eunit suite=gen_q_port_tests)
