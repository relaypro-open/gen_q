#include "q2e.h"
#include "ei_util.h"
#include "gen_q_log.h"

// encoders
int ei_x_encode_k_tv(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_datetime(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_time(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_ki(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts);
int ei_x_encode_kj(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts);
int ei_x_encode_kf(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts);
int ei_x_encode_kc(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_ke(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_kh(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_kg(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts);
int ei_x_encode_ks(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_kstring(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);

#define EI_X_ENCODE_SAME_LIST(TYPE, ENCODER, ACCESSOR)  \
    int ei_x_encode_same_list_##TYPE(ei_x_buff* types,   \
            ei_x_buff* values,                          \
            const char* t,                              \
            K r,                                        \
            QOpts* opts) {                              \
                                                        \
        EI(ei_x_encode_tuple_header(types, 2));         \
        EI(ei_x_encode_atom(types, "list"));            \
        EI(ei_x_encode_atom(types, t));                 \
                                                        \
        if(r->n > 0) {                                  \
            EI(ei_x_encode_list_header(values, r->n));  \
            int i;                                      \
            for(i=0; i<r->n; ++i) {                     \
                EI(ENCODER(values, ACCESSOR(r)[i]));    \
            }                                           \
        }                                               \
        EI(ei_x_encode_empty_list(values));             \
        return 0;                                       \
    }

EI_X_ENCODE_SAME_LIST(symbol, ei_x_encode_atom, kS);
EI_X_ENCODE_SAME_LIST(integer, ei_x_encode_long, kI);
EI_X_ENCODE_SAME_LIST(long, ei_x_encode_longlong, kJ);
EI_X_ENCODE_SAME_LIST(short, ei_x_encode_long, kH);
EI_X_ENCODE_SAME_LIST(real, ei_x_encode_double, kE);
EI_X_ENCODE_SAME_LIST(float, ei_x_encode_double, kF);
int ei_x_encode_same_list_byte(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts);
int ei_x_encode_same_list_datetime(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);
int ei_x_encode_same_list_time(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);

// helpers
int msec_to_sec(int s);
long long datetime_to_unix_timestamp(double d);

int ei_x_encode_general_list(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts);

int ei_x_encode_k(ei_x_buff* x, K r, QOpts* opts) {
    EI(ei_x_encode_tuple_header(x, 2));
    ei_x_buff* types = x;
    ei_x_buff values;
    EI(ei_x_new(&values));
    EIC(ei_x_encode_k_tv(types, &values, r, opts),
            ei_x_free(&values)); // cleanup expression
    EIC(ei_x_append(types, &values),
            ei_x_free(&values)); // cleanup expression
    EI(ei_x_free(&values));
    return 0;
}

int ei_x_encode_k_tv(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    if(!r) {
        LOG("null K object! %d\n", 0);
        return -1;
    }

    LOG("ei_x_encode_k_tv for type %d\n", r->t);
    switch(r->t) {
        case -128:
            // errors should be handled externally
            return -1;
        case -KT: // time
            EI(ei_x_encode_time(types, values, r, opts));
            return 0;
        case -KV: // second
            EI(ei_x_encode_ki(types, values, "second", r, opts));
            return 0;
        case -KU: // minute
            EI(ei_x_encode_ki(types, values, "minute", r, opts));
            return 0;
        case -KN: // timespan
            EI(ei_x_encode_kj(types, values, "timespan", r, opts));
            return 0;
        case -KZ: // datetime
            EI(ei_x_encode_datetime(types, values, r, opts));
            return 0;
        case -KD: // date
            EI(ei_x_encode_ki(types, values, "date", r, opts));
            return 0;
        case -KM: // month
            EI(ei_x_encode_ki(types, values, "month", r, opts));
            return 0;
        case -KI: // int
            EI(ei_x_encode_ki(types, values, "integer", r, opts));
            return 0;
        case -KP: // timestamp
            EI(ei_x_encode_kj(types, values, "timestamp", r, opts));
            return 0;
        case -KJ: // long
            EI(ei_x_encode_kj(types, values, "long", r, opts));
            return 0;
        case -KF: // float
            EI(ei_x_encode_kf(types, values, "float", r, opts));
            return 0;
        case -KC: // char
            EI(ei_x_encode_kc(types, values, r, opts));
            return 0;
        case -KE: // real
            EI(ei_x_encode_ke(types, values, r, opts));
            return 0;
        case -KH: // short
            EI(ei_x_encode_kh(types, values, r, opts));
            return 0;
        case -KG: // byte
            EI(ei_x_encode_kg(types, values, "byte", r, opts));
            return 0;
        case -KB: // boolean
            EI(ei_x_encode_kg(types, values, "boolean", r, opts));
            return 0;
        case -KS:
            EI(ei_x_encode_ks(types, values, r, opts));
            return 0;
        case 0:
            EI(ei_x_encode_general_list(types, values, r, opts));
            return 0;
        case KS:
            EI(ei_x_encode_same_list_symbol(types, values, "symbol", r, opts));
            return 0;
        case KI:
            EI(ei_x_encode_same_list_integer(types, values, "integer", r, opts));
            return 0;
        case KU:
            EI(ei_x_encode_same_list_integer(types, values, "minute", r, opts));
            return 0;
        case KV:
            EI(ei_x_encode_same_list_integer(types, values, "second", r, opts));
            return 0;
        case KD:
            EI(ei_x_encode_same_list_integer(types, values, "date", r, opts));
            return 0;
        case KM:
            EI(ei_x_encode_same_list_integer(types, values, "month", r, opts));
            return 0;
        case KT:
            EI(ei_x_encode_same_list_time(types, values, r, opts));
            return 0;
        case KJ:
            EI(ei_x_encode_same_list_long(types, values, "long", r, opts));
            return 0;
        case KN:
            EI(ei_x_encode_same_list_long(types, values, "timespan", r, opts));
            return 0;
        case KP:
            EI(ei_x_encode_same_list_long(types, values, "timestamp", r, opts));
            return 0;
        case KG:
            EI(ei_x_encode_same_list_byte(types, values, "byte", r, opts));
            return 0;
        case KB:
            EI(ei_x_encode_same_list_byte(types, values, "boolean", r, opts));
            return 0;
        case KH:
            EI(ei_x_encode_same_list_short(types, values, "short", r, opts));
            return 0;
        case KE:
            EI(ei_x_encode_same_list_real(types, values, "real", r, opts));
            return 0;
        case KF:
            EI(ei_x_encode_same_list_float(types, values, "float", r, opts));
            return 0;
        case KZ:
            EI(ei_x_encode_same_list_datetime(types, values, r, opts));
            return 0;
        case KC:
            EI(ei_x_encode_kstring(types, values, r, opts));
            return 0;
    }
    return -1;
}

int ei_x_encode_time(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "time"));
    int v = r->i;
    if(opts->day_seconds_is_q_time) {
        v = msec_to_sec(v);
    }
    EI(ei_x_encode_long(values, v));
    return 0;
}

int ei_x_encode_datetime(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    if(opts->unix_timestamp_is_q_datetime) {
        long v = datetime_to_unix_timestamp(r->f);
        EI(ei_x_encode_atom(types, "datetime"));
        EI(ei_x_encode_long(values, v));
    } else {
        EI(ei_x_encode_kf(types, values, "datetime", r, opts));
    }
    return 0;
}

int ei_x_encode_ki(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, t));
    EI(ei_x_encode_long(values, r->i));
    return 0;
}

int ei_x_encode_kj(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, t));
    EI(ei_x_encode_longlong(values, r->j));
    return 0;
}

int ei_x_encode_kf(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, t));
    EI(ei_x_encode_double(values, r->f));
    return 0;
}

int ei_x_encode_ke(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "real"));
    EI(ei_x_encode_double(values, (double)r->e));
    return 0;
}

int ei_x_encode_kc(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "char"));
    EI(ei_x_encode_char(values, (char)r->g));
    return 0;
}

int ei_x_encode_kh(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "short"));
    EI(ei_x_encode_long(values, r->h));
    return 0;
}

int ei_x_encode_kg(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, t));
    EI(ei_x_encode_char(values, r->g));
    return 0;
}

int ei_x_encode_ks(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "symbol"));
    EI(ei_x_encode_atom(values, r->s));
    return 0;
}

int ei_x_encode_general_list(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    LOG("ei_x_encode_general_list length %lld\n", r->n);

    EI(ei_x_encode_tuple_header(types, 2));
    EI(ei_x_encode_atom(types, "list"));

    if(r->n > 0) {
        int all_strings = 1;
        EI(ei_x_encode_list_header(values, r->n));
        int i;
        for(i=0; i<r->n; ++i) {
            K elem = kK(r)[i];
            if(elem->t != KC) {
                all_strings = 0;
                break;
            }
            EI(ei_x_encode_string_len(values, (const char*)kC(elem), elem->n));
        }

        if(all_strings) {
            EI(ei_x_encode_atom(types, "string"));
        } else {
            EI(ei_x_encode_list_header(types, r->n));
            int j;
            for(j=0; j<i; ++j) {
                EI(ei_x_encode_atom(types, "string"));
            }

            for(; i<r->n; ++i) {
                EI(ei_x_encode_k_tv(types, values, kK(r)[i], opts));
            }
            EI(ei_x_encode_empty_list(types));
        }
    } else {
        EI(ei_x_encode_empty_list(types));
    }
    EI(ei_x_encode_empty_list(values));
    return 0;
}

int ei_x_encode_same_list_byte(ei_x_buff* types, ei_x_buff* values, const char* t, K r, QOpts* opts) {
    EI(ei_x_encode_tuple_header(types, 2));
    EI(ei_x_encode_atom(types, "list"));
    EI(ei_x_encode_atom(types, t));
    if(r->n == 0) {
        EI(ei_x_encode_empty_list(values));
    } else {
        EI(ei_x_encode_string_len(values, (const char*)kG(r), r->n));
    }
    return 0;
}

int ei_x_encode_same_list_datetime(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    if(opts->unix_timestamp_is_q_datetime) {
        EI(ei_x_encode_tuple_header(types, 2));
        EI(ei_x_encode_atom(types, "list"));
        EI(ei_x_encode_atom(types, "datetime"));

        if(r->n > 0) {
            EI(ei_x_encode_list_header(values, r->n));
            int i;
            for(i=0; i<r->n; ++i) {
                EI(ei_x_encode_longlong(values, datetime_to_unix_timestamp(kF(r)[i])));
            }
        }
        EI(ei_x_encode_empty_list(values));
    } else {
        EI(ei_x_encode_same_list_float(types, values, "datetime", r, opts));
    }
    return 0;
}

int ei_x_encode_same_list_time(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    if(opts->day_seconds_is_q_time) {
        EI(ei_x_encode_tuple_header(types, 2));
        EI(ei_x_encode_atom(types, "list"));
        EI(ei_x_encode_atom(types, "time"));

        if(r->n > 0) {
            EI(ei_x_encode_list_header(values, r->n));
            int i;
            for(i=0; i<r->n; ++i) {
                EI(ei_x_encode_long(values, msec_to_sec(kI(r)[i])));
            }
        }
        EI(ei_x_encode_empty_list(values));
    } else {
        EI(ei_x_encode_same_list_integer(types, values, "time", r, opts));
    }
    return 0;
}

int ei_x_encode_kstring(ei_x_buff* types, ei_x_buff* values, K r, QOpts* opts) {
    EI(ei_x_encode_atom(types, "string"));
    if(r->n == 0) {
        EI(ei_x_encode_empty_list(values));
    } else {
        EI(ei_x_encode_string_len(values, (const char*)kC(r), r->n));
    }
    return 0;
}

int msec_to_sec(int s) {
    return s / 1000;
}

long long datetime_to_unix_timestamp(double d) {
    return (long long)((d+10957)*8.64e4);
}
