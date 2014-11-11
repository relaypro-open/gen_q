-define(assertNeighborhood(Expect, Expr, Epsilon),
    begin
        ((fun (__X) ->
            __V = (Expr),
            case abs(__V-__X) < Epsilon of
                true -> ok;
                false -> erlang:error({assertNeighborhood_failed,
                                            [{module, ?MODULE},
                                                {line, ?LINE},
                                                {expression, (??Expr)},
                                                {expected, {Expect, '+/-', Epsilon}},
                                                {value, __V}]})
                            end
                    end)(Expect))
    end).
-define(_assertNeighborhood(Expect, Expr, Epsilon), ?_test(?assertNeighborhood(Expect, Expr, Epsilon))).

-define(assertAllNeighborhood(Expect, Expr, Epsilon),
    begin
        ((fun (__X) ->
            __V = (Expr),
            case lists:all(fun({__X1, __V1}) ->
                            abs(__V1-__X1) < Epsilon
                    end, lists:zip(__X, __V)) of
                true -> ok;
                false -> erlang:error({assertAllNeighborhood_failed,
                                            [{module, ?MODULE},
                                                {line, ?LINE},
                                                {expression, (??Expr)},
                                                {expected, {Expect, '+/-', Epsilon}},
                                                {value, __V}]})
                            end
                    end)(Expect))
    end).
-define(_assertAllNeighborhood(Expect, Expr, Epsilon), ?_test(?assertAllNeighborhood(Expect, Expr, Epsilon))).

-define(_eqe(Type, Value), ?_assertMatch({ok, {Type, Value}}, gen_q_port:apply(P, H, "(::)", Type, Value))).
-define(_eqe_value(Type, InValue, OutValue), ?_assertMatch({ok, {Type, OutValue}}, gen_q_port:apply(P, H, "(::)", Type, InValue))).
-define(_eqe_type(InType, OutType, Value), ?_assertMatch({ok, {OutType, Value}}, gen_q_port:apply(P, H, "(::)", InType, Value))).

-define(eqe_neighbor(Type, Value, Eps),
          begin
                  ((fun (__ExpectType, __ExpectValue) ->
                      case gen_q_port:apply(P, H, "(::)", Type, Value) of
                          {ok, {__ExpectType, __Value2}} ->
                              ?assertNeighborhood(__ExpectValue, __Value2, Eps);
                          {ok, {__Type2, _}} ->
                              erlang:error({'eqe_neighbor_type_failed',
                                      [{module, ?MODULE},
                                          {line, ?LINE},
                                          {expression, {(??Type), (??Value)}},
                                          {expected, __ExpectType},
                                          {value, __Type2}]});
                          __OtherResult ->
                              erlang:error({'eqe_neighbor_failed',
                                      [{module, ?MODULE},
                                          {line, ?LINE},
                                          {expression, {(??Type), (??Value)}},
                                          {result, __OtherResult}]})
                      end
              end)(Type, Value))
    end).
-define(_eqe_neighbor(Type, Value, Eps), ?_test(?eqe_neighbor(Type, Value, Eps))).

-define(eqe_all_neighbor(Type, Value, Eps),
    begin
            ((fun (__ExpectType, __ExpectValue) ->
                case gen_q_port:apply(P, H, "(::)", Type, Value) of
                    {ok, {__ExpectType, __Value2}} ->
                        ?assertAllNeighborhood(__ExpectValue, __Value2, Eps);
                    {ok, {__Type2, _}} ->
                        erlang:error({'eqe_all_neighboor_failed',
                                [{module, ?MODULE},
                                 {line, ?LINE},
                                 {expression, {(??Type), (??Value)}},
                                 {expected, __ExpectType},
                                 {value, __Type2}]});
                    __OtherResult ->
                        erlang:error({'eqe_all_neighbor_failed',
                                [{module, ?MODULE},
                                    {line, ?LINE},
                                    {expression, {(??Type), (??Value)}},
                                    {result, __OtherResult}]})
                end
        end)(Type, Value))
    end).
-define(_eqe_all_neighbor(Type, Value, Eps), ?_test(?eqe_all_neighbor(Type, Value, Eps))).
