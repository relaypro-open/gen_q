#include "e2q.h"
#include "gen_q.h"
#include "ei_util.h"
#include "gen_q_log.h"
#include <stdlib.h>
#include <string.h>

#define STR_EQUAL(s0,s1) 0==strcmp((char*)s0,(char*)s1)
#define K_STR 25

#define EI_NULL_OR_INFINITY(var, null_val, inf_val)          \
    do {                                                     \
        int etype = 0;                                       \
        int atom_size = 0;                                   \
        EI(ei_get_type(b, i, &etype, &atom_size));           \
        if(etype == ERL_ATOM_EXT) {                          \
            unsigned char* atom = genq_alloc(sizeof(unsigned char)*(atom_size+1)); \
            EIC(ei_decode_atom_safe(b, i, atom), genq_free(atom));     \
            if(is_null(atom)) {                              \
                *var = null_val;                             \
            } else if(is_infinity(atom)) {                   \
                *var = inf_val;                              \
            } else {                                         \
                LOG("ERROR unknown atom %s\n", atom);        \
                genq_free(atom);                                  \
                return -1;                                   \
            }                                                \
            genq_free(atom);                                      \
            return 0;                                        \
        }                                                    \
    } while(0)

// types
int get_type_identifier_from_string(const unsigned char* str, int* type);
int ei_get_k_type(char* buff, int* ti, int* type, int* size);
int ei_get_k_type_from_atom(char* buff, int* ti, int atom_size, int* type);

// decoders
int ei_decode_ki(char* b, int* i, int* ki, QOpts* opts);
int ei_decode_kj(char* b, int* i, long long* kj, QOpts* opts);
int ei_decode_kh(char* b, int* i, short* kh, QOpts* opts);
int ei_decode_kf(char* b, int* i, double* kf, QOpts* opts);
int ei_decode_ke(char* b, int* i, float* ke, QOpts* opts);
int ei_decode_kc(char* b, int* i, unsigned char* kc, QOpts* opts);
int ei_decode_kg(char* b, int* i, unsigned char* kg, QOpts* opts);
int ei_decode_kb(char* b, int* i, unsigned char* kb, QOpts* opts);
int ei_decode_datetime(char* b, int* i, double* dt, QOpts* opts);
int ei_decode_time(char* b, int* i, int* ki, QOpts* opts);
int ei_decode_ks(char* b, int* i, K* k, QOpts* opts);
int ei_decode_general_list(char* b, int* ti, int* vi, K* k, QOpts* opts);
int ei_decode_same_list(char* b, int* i, int ktype, K* k, QOpts* opts);
int ei_decode_and_assign(char* b, int* i, int ktype, K* k, int list_index, QOpts* opts);
int ei_decode_table(char* b, int* ti, int* vi, K* k, QOpts* opts);
int ei_decode_dict(char* b, int* ti, int* vi, K* k, QOpts* opts);
int ei_decode_unknown(char* b, int* ti, int* vi, K* k, int ktype, QOpts* opts);
int ei_assign_from_string(K* k, int ktype, int arity, unsigned char* v, QOpts* opts);
int ei_assign_index_from_string(K* k, int ktype, int kindex, unsigned char* v, int vindex, QOpts* opts);

// helpers
int is_exact_atom(unsigned char* atom, const char* compare);
int is_infinity(unsigned char* atom);
int is_null(unsigned char* atom);
int is_false(unsigned char* atom);
int is_true(unsigned char* atom);
void r02(K k1, K k2);
void safe_deref_list(K k, int ktype, int j, int n);
void safe_deref_list_and_free(K k, int ktype, int j, int n, void* f);
void safe_deref_list2(K k1, int ktype1, int j1, int n1, K k2, int ktype2, int j2, int n2);
double unix_timestamp_to_datetime(long long u);
int sec_to_msec(int sec);

int ei_get_k_type(char* buff, int* ti, int* type, int* size) {
    int ttype = 0; // erlang type of ktype element
    int tsize = 0; // erlang size of ktype element
    EI(ei_get_type(buff, ti, &ttype, &tsize));
    if(ttype == ERL_SMALL_TUPLE_EXT ||
            ttype == ERL_LARGE_TUPLE_EXT) {
        // handle complex type: e.g. list, table, dict, projection
        // The first atom in the tuple defines the container type
        EI(ei_decode_tuple_header(buff, ti, &tsize));
        EI(ei_get_type(buff, ti, &ttype, &tsize));
        // ti now pointing at the first element of the tuple.
        // ttype has the type of the first element of the tuple.
        // tsize has the length of the first element of the tuple
        // upon return from this function, ti will point to the
        // second element in the tuple
    }

    if(ttype == ERL_INTEGER_EXT || ttype == ERL_SMALL_INTEGER_EXT) {
        long v = 0;
        EI(ei_decode_long(buff, ti, &v));
        *type = v;
        return 0;
    }

    if(ttype != ERL_ATOM_EXT) {
        LOG("ERROR type is not an atom %d\n", ttype);
        return -1;
    }

    EI(ei_get_k_type_from_atom(buff, ti, tsize, type));
    return 0;
}

int ei_get_k_type_from_atom(char* buff, int* ti, int atom_size, int* type) {
    unsigned char* atom = genq_alloc(sizeof(unsigned char)*(atom_size+1));
    atom[atom_size] = '\0';
    EIC(ei_decode_atom_safe(buff, ti, atom), genq_free(atom));
    EIC(get_type_identifier_from_string(atom, type), genq_free(atom));
    genq_free(atom);
    return 0;
}

int get_type_identifier_from_string(const unsigned char* str, int *type) {
    // Roughly ordered by probability of use
    // returns 0 for "list"
    // returns 101 for "ok"
    if(STR_EQUAL("integer",str)) {
        *type = -KI;
    } else if(STR_EQUAL("symbol",str)) {
        *type = -KS;
    } else if(STR_EQUAL("month",str)) {
        *type = -KM;
    } else if(STR_EQUAL("list",str)) {
        *type = 0;
    } else if(STR_EQUAL("table",str)) {
        *type = XT;
    } else if(STR_EQUAL("dict",str)) {
        *type = XD;
    } else if(STR_EQUAL("ok",str)) {
        *type = 101;
    } else if(STR_EQUAL("undefined",str)) {
        *type = 101;
    } else if(STR_EQUAL("boolean",str)) {
        *type = -KB;
    } else if(STR_EQUAL("float",str)) {
        *type = -KF;
    } else if(STR_EQUAL("string",str)) {
        *type = K_STR;
    } else if(STR_EQUAL("datetime",str)) {
        *type = -KZ;
    } else if(STR_EQUAL("long",str)) {
        *type = -KJ;
    } else if(STR_EQUAL("time",str)) {
        *type = -KT;
    } else if(STR_EQUAL("date",str)) {
        *type = -KD;
    } else if(STR_EQUAL("second",str)) {
        *type = -KV;
    } else if(STR_EQUAL("byte",str)) {
        *type = -KG;
    } else if(STR_EQUAL("minute",str)) {
        *type = -KU;
    } else if(STR_EQUAL("char",str)) {
        *type = -KC;
    } else if(STR_EQUAL("real",str)) {
        *type = -KE;
    } else if(STR_EQUAL("short",str)) {
        *type = -KH;
    } else if(STR_EQUAL("timespan",str)) {
        *type = -KN;
    } else if(STR_EQUAL("timestamp",str)) {
        *type = -KP;
    } else if(STR_EQUAL("projection", str)) {
        *type = 104;
    } else {
        LOG("ERROR unknown type string %s\n", str);
        return -1;
    }
    return 0;
}

int ei_decode_types_values_tuple(char *buff, int *index, int *types_index, int *values_index) {
    int arity = 0;
    EI(ei_decode_tuple_header(buff, index, &arity));
    if(arity != 2) {
        LOG("ERROR decode_types_values arity %d\n", arity);
        return -1;
    }
    *types_index = *index;
    EI(ei_skip_term(buff, index));
    *values_index = *index;
    EI(ei_skip_term(buff, index));
    return 0;
}

int ei_decode_k(char *buff, int* types_index, int* values_index, K* k, QOpts* opts) {
    int ktype = 0;
    int esize = 0;
    EI(ei_get_k_type(buff, types_index, &ktype, &esize));
    switch(ktype) {

       case -KT: // time
       {
           *k = kt(0);
           EI(ei_decode_time(buff, values_index, &(*k)->i, opts));
           break;
       }
       case -KV: // second
           *k = ki(0);
           (*k)->t = -KV;
           EI(ei_decode_ki(buff, values_index, &(*k)->i, opts));
           break;
       case -KU: // minute
           *k = ki(0);
           (*k)->t = -KU;
           EI(ei_decode_ki(buff, values_index, &(*k)->i, opts));
           break;
       case -KN: // timespan
           *k = kj(0);
           (*k)->t = -KN;
           EI(ei_decode_kj(buff, values_index, &(*k)->j, opts));
           break;
       case -KZ: // datetime
           *k = kz(0);
           EI(ei_decode_datetime(buff, values_index, &(*k)->f, opts));
           break;
       case -KD: // date
           *k = kd(0);
           EI(ei_decode_ki(buff, values_index, &(*k)->i, opts));
           break;
       case -KM: // month
           *k = ki(0);
           (*k)->t = -KM;
           EI(ei_decode_ki(buff, values_index, &(*k)->i, opts));
           break;
       case -KP: // timestamp
           *k = kj(0);
           (*k)->t = -KP;
           EI(ei_decode_kj(buff, values_index, &(*k)->j, opts));
           break;
       case -KC: // char
           *k = kc(0);
           EI(ei_decode_kc(buff, values_index, &(*k)->g, opts));
           break;
       case -KF: // float
           *k = kf(0);
           EI(ei_decode_kf(buff, values_index, &(*k)->f, opts));
           break;
       case -KE: // real
           *k = ke(0);
           EI(ei_decode_ke(buff, values_index, &(*k)->e, opts));
           break;
       case -KJ: // long
           *k = kj(0);
           EI(ei_decode_kj(buff, values_index, &(*k)->j, opts));
           break;
       case -KI: // int
           *k = ki(0);
           EI(ei_decode_ki(buff, values_index, &(*k)->i, opts));
           break;
       case -KH: // short
           *k = kh(0);
           EI(ei_decode_kh(buff, values_index, &(*k)->h, opts));
           break;
       case -KG: // byte
           *k = kg(0);
           EI(ei_decode_kg(buff, values_index, &(*k)->g, opts));
           break;
       case -KB: // boolean
           *k = kb(0);
           EI(ei_decode_kb(buff, values_index, &(*k)->g, opts));
           break;

       case -KS: // symbol
           EI(ei_decode_ks(buff, values_index, k, opts));
           break;
       case K_STR:
       {
           unsigned char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(buff, values_index, &s, &len));
           *k = ktn(KC, len);
           int i;
           for(i=0; i<len; ++i) {
               kC(*k)[i] = s[i];
           }
           genq_free(s);
           break;
       }
       case 0:   // any list - this differs from normal q type checking
           EI(ei_decode_general_list(buff, types_index, values_index, k, opts));
           break;
       case XT:  // table
           EI(ei_decode_table(buff, types_index, values_index, k, opts));
           break;
       case XD:  // dict/keyed table
           EI(ei_decode_dict(buff, types_index, values_index, k, opts));
           break;
       case 101: // (::)
           *k = 0;
           break;
       case 104:
           EI(ei_decode_unknown(buff, types_index, values_index, k, ktype, opts));
           break;
       default:
           LOG("ERROR unhandled type %d\n", ktype);
           /*
           EI(ei_decode_unknown(buff, types_index, values_index, k, ktype, opts));
           break;
           */
           return -1;
    }
    return 0;
}

// decoders
int ei_decode_ks(char* b, int* i, K* k, QOpts* opts) {
    unsigned char* s = NULL;
    int len = 0;
    EI(ei_decode_alloc_string(b, i, &s, &len));
    *k = ks((char*)s);
    genq_free(s);
    return 0;
}

int ei_decode_ki(char* b, int* i, int* ki, QOpts* opts) {
    EI_NULL_OR_INFINITY(ki, ni, wi);

    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *ki = (int)v;
    return 0;
}

int ei_decode_kj(char* b, int* i, long long* kj, QOpts* opts) {
    EI_NULL_OR_INFINITY(kj, nj, wj);

    EI(ei_decode_longlong(b, i, kj));
    return 0;
}

int ei_decode_kh(char* b, int* i, short* kh, QOpts* opts) {
    EI_NULL_OR_INFINITY(kh, nh, wh);

    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *kh = (short)v;
    return 0;
}

int ei_decode_kf(char* b, int* i, double* kf, QOpts* opts) {
    EI_NULL_OR_INFINITY(kf, nf, wf);
    int type = 0;
    int arity = 0;
    EI(ei_get_type(b, i, &type, &arity));
    if(type == ERL_FLOAT_EXT) {
        EI(ei_decode_double(b, i, kf));
    } else {
        long long v = 0;
        EI(ei_decode_longlong(b, i, &v));
        *kf = (double)v;
    }
    return 0;
}

int ei_decode_ke(char* b, int* i, float* ke, QOpts* opts) {
    int type = 0;
    int arity = 0;
    EI(ei_get_type(b, i, &type, &arity));
    if(type == ERL_FLOAT_EXT) {
        double v = 0;
        EI(ei_decode_double(b, i, &v));
        *ke = (float)v;
    } else {
        long v = 0;
        EI(ei_decode_long(b, i, &v));
        *ke = (float)v;
    }
    return 0;
}

int ei_decode_kc(char* b, int* i, unsigned char* kc, QOpts* opts) {
    unsigned char c = 0;
    EI(ei_decode_char_safe(b, i, &c));
    *kc = c;
    return 0;
}

int ei_decode_kg(char* b, int* i, unsigned char* kg, QOpts* opts) {
    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *kg = (unsigned char)v;
    return 0;
}

int ei_decode_kb(char* b, int* i, unsigned char* kb, QOpts* opts) {
    int etype = 0;
    int atom_size = 0;
    EI(ei_get_type(b, i, &etype, &atom_size));
    if(etype == ERL_ATOM_EXT) {
        unsigned char* atom = genq_alloc(sizeof(unsigned char)*(atom_size+1));
        EIC(ei_decode_atom_safe(b, i, atom), genq_free(atom));
        if(is_false(atom)) {
            *kb = 0;
        } else if(is_true(atom)) {
            *kb = 1;
        }
        genq_free(atom);
        return 0;
    }
    EI(ei_decode_kg(b, i, kb, opts));
    return 0;
}

int ei_decode_datetime(char* b, int* i, double* dt, QOpts* opts) {
    EI_NULL_OR_INFINITY(dt, nf, wf);

    if(opts->unix_timestamp_is_q_datetime) {
        long long v = 0;
        EI(ei_decode_longlong(b, i, &v));
        *dt = unix_timestamp_to_datetime(v);
    } else {
        int type = 0;
        int arity = 0;
        EI(ei_get_type(b, i, &type, &arity));
        if(type == ERL_FLOAT_EXT) {
            EI(ei_decode_double(b, i, dt));
        } else {
            long long v = 0;
            EI(ei_decode_longlong(b, i, &v));
            *dt = v;
        }
    }
    return 0;
}

int ei_decode_time(char* b, int* i, int* ki, QOpts* opts) {
    EI(ei_decode_ki(b, i, ki, opts));
    if(opts->day_seconds_is_q_time) {
        *ki = sec_to_msec(*ki);
    }
    return 0;
}

int ei_decode_general_list(char* b, int* ti, int* vi, K* k, QOpts* opts) {
    int ttype = 0;
    int tsize = 0;
    EI(ei_get_type(b, ti, &ttype, &tsize));
    if(ttype == ERL_ATOM_EXT) {
        int ktype = 0;
        int esize = 0;
        EI(ei_get_k_type(b, ti, &ktype, &esize));
        EI(ei_decode_same_list(b, vi, ktype, k, opts));
        return 0;
    } else if(ttype == ERL_LIST_EXT ||
            ttype == ERL_NIL_EXT) {
        int vtype = 0;
        int vsize = 0;
        EI(ei_get_type(b, vi, &vtype, &vsize));
        if(vtype == ERL_STRING_EXT) {
            LOG("mixed list decoded as erlang string %d\n", 0);
            EI(ei_decode_list_header(b, ti, &tsize));
            if(tsize != vsize) {
                LOG("ERROR general list size (string) mismatch tsize=%d, vsize=%d\n", tsize, vsize);
                return -1;
            }

            unsigned char* s = genq_alloc(sizeof(char)*(vsize+1));
            EIC(ei_decode_string_safe(b, vi, s), genq_free(s));

            *k = ktn(0, vsize);
            int list_index;
            for(list_index=0; list_index < vsize; ++list_index) {
                int ktype = 0;
                int ktypesize = 0;
                EIC(ei_get_k_type(b, ti, &ktype, &ktypesize), genq_free(s));
                EIC(ei_assign_index_from_string(k, ktype, list_index, s, list_index, opts),
                        safe_deref_list_and_free(*k, 0, list_index, vsize, s));
            }

            genq_free(s);
            return 0;

        } else if (vtype == ERL_LIST_EXT ||
                vtype == ERL_NIL_EXT) {
            EI(ei_decode_list_header(b, ti, &tsize));
            EI(ei_decode_list_header(b, vi, &vsize));
            if(tsize != vsize) {
                LOG("ERROR general list size (list) mismatch tsize=%d, vsize=%d\n", tsize, vsize);
                return -1;
            }
            *k = ktn(0, vsize);
            int list_index;
            for(list_index=0; list_index<vsize; ++list_index) {
                K k_elem = 0;
                EIC(ei_decode_k(b, ti, vi, &k_elem, opts),
                        safe_deref_list(*k, 0, list_index, vsize));
                kK(*k)[list_index] = k_elem;
            }
            if(tsize > 0) {
                EIC(ei_skip_term(b, ti), r0(*k)); // skip tail
                EIC(ei_skip_term(b, vi), r0(*k)); // skip tail
            }
            return 0;
        }
        LOG("ERROR general list value format unknown %d\n", vtype);
        return -1;
    }
    LOG("ERROR general list format unknown %d\n", ttype);
    return -1;
}

int ei_assign_from_string(K* k, int ktype, int arity, unsigned char* v, QOpts* opts) {
    *k = ktn(-ktype, arity);
    int list_index;
    if(ktype == -KT && opts->day_seconds_is_q_time) {
        for(list_index=0; list_index < arity; ++list_index) {
            kI(*k)[list_index] = sec_to_msec(v[list_index]);
        }
    } else if(ktype == -KZ && opts->unix_timestamp_is_q_datetime) {
        for(list_index=0; list_index < arity; ++list_index) {
            kF(*k)[list_index] = unix_timestamp_to_datetime(v[list_index]);
        }
    } else {
        for(list_index=0; list_index < arity; ++list_index) {
            switch(ktype) {
                case -KT:
                case -KV:
                case -KU:
                case -KI:
                case -KD:
                case -KM:
                    kI(*k)[list_index] = v[list_index];
                    break;
                case -KN:
                case -KP:
                case -KJ:
                    kJ(*k)[list_index] = v[list_index];
                    break;
                case -KG:
                case -KB:
                    kG(*k)[list_index] = v[list_index];
                    break;
                case -KH:
                    kH(*k)[list_index] = v[list_index];
                    break;
                case -KC:
                    kC(*k)[list_index] = v[list_index];
                    break;
                case -KF:
                case -KZ:
                    kF(*k)[list_index] = v[list_index];
                    break;
                case -KE:
                    kE(*k)[list_index] = v[list_index];
                    break;
                default:
                    LOG("ERROR assign from string, unhandled type %d\n", ktype);
                    return -1;
            }
        }
    }
    return 0;
}

int ei_assign_index_from_string(K* k, int ktype, int kindex, unsigned char* v, int vindex, QOpts* opts) {
    if(ktype == -KT && opts->day_seconds_is_q_time) {
        kK(*k)[kindex] = ki(sec_to_msec(v[vindex]));
        kK(*k)[kindex]->t = -KT;
    } else if(ktype == -KZ && opts->unix_timestamp_is_q_datetime) {
        kK(*k)[kindex] = kj(unix_timestamp_to_datetime(v[vindex]));
        kK(*k)[kindex]->t = -KZ;
    } else {
        switch(ktype) {
            case -KT:
            case -KV:
            case -KU:
            case -KI:
            case -KD:
            case -KM:
                kK(*k)[kindex] = ki(v[vindex]);
                break;
            case -KN:
            case -KP:
            case -KJ:
                kK(*k)[kindex] = kj(v[vindex]);
                break;
            case -KG:
            case -KB:
                kK(*k)[kindex] = kg(v[vindex]);
                break;
            case -KH:
                kK(*k)[kindex] = kh(v[vindex]);
                break;
            case -KC:
                kK(*k)[kindex] = kc(v[vindex]);
                break;
            case -KF:
            case -KZ:
                kK(*k)[kindex] = kf(v[vindex]);
                break;
            case -KE:
                kK(*k)[kindex] = ke(v[vindex]);
                break;
            default:
                LOG("ERROR assign index from string, unhandled type %d\n", ktype);
                return -1;
        }
        kK(*k)[kindex]->t = ktype;
    }
    return 0;
}

int ei_decode_same_list(char* b, int* i, int ktype, K* k, QOpts* opts) {
    int type = 0;
    int arity = 0;
    EI(ei_get_type(b, i, &type, &arity));
    if(type == ERL_STRING_EXT) {
        // erlang encodes some lists of integers as strings

        unsigned char* v = genq_alloc(sizeof(unsigned char)*(arity+1));
        EIC(ei_decode_string_safe(b, i, v), genq_free(v));
        EIC(ei_assign_from_string(k, ktype, arity, v, opts), genq_free(v));
        genq_free(v);

    } else {

        EI(ei_decode_list_header(b, i, &arity));

        if(ktype == K_STR) {
            *k = ktn(0, arity);
        } else {
            *k = ktn(-ktype, arity);
        }

        int list_index;
        for(list_index=0; list_index < arity; ++list_index) {
            EIC(ei_decode_and_assign(b, i, ktype, k, list_index, opts),
                    safe_deref_list(*k, 0, list_index, arity));
        }
        if(arity > 0) {
            EIC(ei_skip_term(b, i), r0(*k)); // skip tail
        }
    }
    return 0;
}

int ei_decode_and_assign(char* b, int* i, int ktype, K* k, int list_index, QOpts* opts) {
    switch(ktype) {

       case -KT: // time
           EI(ei_decode_time(b, i, &kI(*k)[list_index], opts));
           break;
       case -KV: // second
       case -KU: // minute
       case -KD: // date
       case -KM: // month
       case -KI: // int
           EI(ei_decode_ki(b, i, &kI(*k)[list_index], opts));
           break;
       case -KN: // timespan
       case -KP: // timestamp
       case -KJ: // long
           EI(ei_decode_kj(b, i, &kJ(*k)[list_index], opts));
           break;
       case -KZ: // datetime
           EI(ei_decode_datetime(b, i, &kF(*k)[list_index], opts));
           break;
       case -KF: // float
           EI(ei_decode_kf(b, i, &kF(*k)[list_index], opts));
           break;
       case -KC: // char
           EI(ei_decode_kc(b, i, &kC(*k)[list_index], opts));
           break;
       case -KE: // real
           EI(ei_decode_ke(b, i, &kE(*k)[list_index], opts));
           break;
       case -KH: // short
           EI(ei_decode_kh(b, i, &kH(*k)[list_index], opts));
           break;
       case -KG: // byte
           EI(ei_decode_kg(b, i, &kG(*k)[list_index], opts));
           break;
       case -KB: // boolean
           EI(ei_decode_kb(b, i, &kG(*k)[list_index], opts));
           break;

       case -KS: // symbol
       {
           unsigned char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(b, i, &s, &len));
           kS(*k)[list_index] = ss((char*)s);
           genq_free(s);
           break;
       }
       case K_STR:
       {
           unsigned char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(b, i, &s, &len));
           K kstr = ktn(KC, len);
           int i;
           for(i=0; i<len; ++i) {
               kC(kstr)[i] = s[i];
           }
           kK(*k)[list_index] = kstr;
           genq_free(s);
           break;
       }
       case 0:   // list
       case XT:  // table
       case XD:  // dict/keyed table
            // This function does not support these types because lists of
            // tables and dicts are always general lists
       default:
        LOG("ERROR decode and assign unhandled type %d\n", ktype);
        return -1;
    }
    return 0;
}

int ei_decode_table(char* b, int* ti, int* vi, K* k, QOpts* opts) {

    //         ti               vi
    //         |                |
    //         v                v
    // {table, {list, QTypes}}, {ColNames,ColValues}

    int varity = 0;
    EI(ei_decode_tuple_header(b, vi, &varity));
    if(varity != 2) {
        LOG("ERROR decode_table bad arity %d\n", varity);
        return -1;
    }

    //         ti                vi
    //         |                 |
    //         v                 v
    // {table, {list, QTypes}}, {ColNames,ColValues}

    // decode column names
    int nvcols = 0;
    EI(ei_decode_list_header(b, vi, &nvcols));

    int list_index;

    K r_cols = ktn(KS, nvcols);
    for(list_index=0; list_index < nvcols; ++list_index) {
        EIC(ei_decode_and_assign(b, vi, -KS, &r_cols, list_index, opts), r0(r_cols));
    }
    if(nvcols > 0) {
        EIC(ei_skip_term(b, vi), r0(r_cols)); // skip tail
    }

    //         ti                         vi
    //         |                          |
    //         v                          v
    // {table, {list, QTypes}}, {ColNames,ColValues}

    // decode column values
    K r_vals;
    EIC(ei_decode_k(b, ti, vi, &r_vals, opts), r0(r_cols));

    *k = xT(xD(r_cols, r_vals));
    return 0;
}

int ei_decode_unknown(char* b, int* ti, int* vi, K* k, int ktype, QOpts* opts) {
    int varity = 0;
    EI(ei_decode_tuple_header(b, vi, &varity));

    *k = ktn(0, varity);
    (*k)->t = ktype;

    int i;
    for(i=0; i<varity; ++i) {
        EIC(ei_decode_k(b, ti, vi, &kK(*k)[i], opts),
                safe_deref_list(*k, 0, i, varity));
    }
    return 0;
}

int ei_decode_dict(char* b, int* ti, int* vi, K* k, QOpts* opts) {

    //        ti                               vi
    //        |                                |
    //        v                                v
    // {dict, {list, symbol}, {list, QTypes}}, {ColNames, ColValues}

    int varity = 0;
    EI(ei_decode_tuple_header(b, vi, &varity));
    if(varity != 2) {
        LOG("ERROR decode_dict bad arity %d\n", varity);
        return -1;
    }

    //        ti                                vi
    //        |                                 |
    //        v                                 v
    // {dict, {list, symbol}, {list, QTypes}}, {ColNames, ColValues}

    // decode key
    K r_key = 0;
    EI(ei_decode_k(b, ti, vi, &r_key, opts));

    //                        ti                          vi
    //                        |                           |
    //                        v                           v
    // {dict, {list, symbol}, {list, QTypes}}, {ColNames, ColValues}

    K r_val = 0;
    EIC(ei_decode_k(b, ti, vi, &r_val, opts), r0(r_key));

    *k = xD(r_key, r_val);
    return 0;
}

// helpers
int is_infinity(unsigned char* atom) {
    return is_exact_atom(atom, "infinity");
}

int is_null(unsigned char* atom) {
    return is_exact_atom(atom, "null");
}
int is_false(unsigned char* atom) {
    return is_exact_atom(atom, "false");
}

int is_true(unsigned char* atom) {
    return is_exact_atom(atom, "true");
}

int is_exact_atom(unsigned char* atom, const char* compare) {
    int result = 0==strcmp((char*)atom,(char*)compare);
    return result;
}

void safe_deref_list_and_free(K k, int ktype, int j, int n, void* f) {
    safe_deref_list(k, ktype, j, n);
    genq_free(f);
}

void safe_deref_list(K k, int ktype, int j, int n) {
    if(ktype != 0) {
        r0(k);
        return;
    }
    // List needs to be initialized before freeing
    int i;
    for(i=j; i<n; ++i) {
        kK(k)[i] = ki(0);
    }
    r0(k);
}

void safe_deref_list2(K k1, int ktype1, int j1, int n1,
        K k2, int ktype2, int j2, int n2) {
    safe_deref_list(k1, ktype1, j1, n1);
    safe_deref_list(k2, ktype2, j2, n2);
}

void r02(K k1, K k2) {
    r0(k1);
    r0(k2);
}

double unix_timestamp_to_datetime(long long u) {
    return u/8.64e4-10957;
}

int sec_to_msec(int sec) {
    return 1000*sec;
}

