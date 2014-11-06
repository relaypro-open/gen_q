#include "q2e.h"
#include "ei_util.h"
#include "gen_q_log.h"

int ei_x_encode_k_tv(ei_x_buff* types, ei_x_buff* values, K r);
int ei_x_encode_ki(ei_x_buff* types, ei_x_buff* values, K r);

int ei_x_encode_general_list(ei_x_buff* types, ei_x_buff* values, K r);

int ei_x_encode_k(ei_x_buff* x, K r) {
    EI(ei_x_encode_tuple_header(x, 2));
    ei_x_buff* types = x;
    ei_x_buff values;
    EI(ei_x_new(&values));
    EIC(ei_x_encode_k_tv(types, &values, r),
            ei_x_free(&values)); // cleanup expression
    EIC(ei_x_append(types, &values),
            ei_x_free(&values)); // cleanup expression
    EI(ei_x_free(&values));
    return 0;
}

int ei_x_encode_k_tv(ei_x_buff* types, ei_x_buff* values, K r) {
    if(!r) {
        LOG("null K object! %d\n", 0);
        return -1;
    }

    LOG("ei_x_encode_k_tv for type %d\n", r->t);
    switch(r->t) {
        case -128:
            // errors should be handled externally
            return -1;
        case -KI:
            EI(ei_x_encode_ki(types, values, r));
            return 0;
        case 0:
            EI(ei_x_encode_general_list(types, values, r));
            return 0;
    }
    return -1;
}

int ei_x_encode_ki(ei_x_buff* types, ei_x_buff* values, K r) {
    EI(ei_x_encode_atom(types, "integer"));
    EI(ei_x_encode_long(values, r->i));
    return 0;
}

int ei_x_encode_general_list(ei_x_buff* types, ei_x_buff* values, K r) {
    LOG("ei_x_encode_general_list length %lld\n", r->n);

    // types
    EI(ei_x_encode_tuple_header(types, 2));
    EI(ei_x_encode_atom(types, "list"));
    EI(ei_x_encode_list_header(types, 2));

    // values
    EI(ei_x_encode_list_header(values, r->n));
    int i;
    for(i=0; i<r->n; ++i) {
        EI(ei_x_encode_k_tv(types, values, kK(r)[i]));
    }
    EI(ei_x_encode_empty_list(types));
    EI(ei_x_encode_empty_list(values));
    return 0;
}
