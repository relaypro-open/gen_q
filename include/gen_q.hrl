-define(APP, gen_q).

-define(LOG(L, F, A), lager:L([{component, ?APP}, {module, ?MODULE}], F, A)).
-define(LOG(L, F), lager:L([{component, ?APP}, {module, ?MODULE}], F)).
