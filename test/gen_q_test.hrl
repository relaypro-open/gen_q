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
