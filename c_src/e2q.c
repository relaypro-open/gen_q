#include "e2q.h"
#include "ei_util.h"
#include "gen_q_log.h"
#include <stdlib.h>
#include <string.h>

#define STR_EQUAL(s0,s1) 0==strcmp(s0,s1)
#define K_STR 25

#define EI_NULL_OR_INFINITY(var, null_val, inf_val)          \
    do {                                                     \
        int etype = 0;                                       \
        int atom_size = 0;                                   \
        EI(ei_get_type(b, i, &etype, &atom_size));           \
        if(etype == ERL_ATOM_EXT) {                          \
            char* atom = malloc(sizeof(char)*(atom_size+1)); \
            EIC(ei_decode_atom(b, i, atom), free(atom));     \
            if(is_null(atom)) {                              \
                *var = null_val;                             \
            } else if(is_infinity(atom)) {                   \
                *var = inf_val;                              \
            } else {                                         \
                free(atom);                                  \
                return -1;                                   \
            }                                                \
            free(atom);                                      \
            return 0;                                        \
        }                                                    \
    } while(0)

// types
int get_type_identifier_from_string(const char* str, int* type);
int ei_get_k_type(char* buff, int* ti, int* vi, int* type, int* size);
int ei_get_k_type_from_atom(char* buff, int* ti, int atom_size, int* type);

// decoders
int ei_decode_ki(char* b, int* i, int* ki, QOpts* opts);
int ei_decode_kj(char* b, int* i, long long* kj, QOpts* opts);
int ei_decode_kh(char* b, int* i, short* kh, QOpts* opts);
int ei_decode_kf(char* b, int* i, double* kf, QOpts* opts);
int ei_decode_ke(char* b, int* i, float* ke, QOpts* opts);
int ei_decode_kc(char* b, int* i, char* kc, QOpts* opts);
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

// helpers
int is_exact_atom(char* atom, const char* compare);
int is_infinity(char* atom);
int is_null(char* atom);
void r02(K k1, K k2);
void safe_deref_list(K k, int ktype, int j, int n);
void safe_deref_list2(K k1, int ktype1, int j1, int n1, K k2, int ktype2, int j2, int n2);
double unix_timestamp_to_datetime(long long u);
int sec_to_msec(int sec);

int ei_get_k_type(char* buff, int* ti, int* vi, int* type, int* size) {
    int ttype = 0; // erlang type of ktype element
    int tsize = 0; // erlang size of ktype element
    EI(ei_get_type(buff, ti, &ttype, &tsize));
    if(ttype == ERL_SMALL_TUPLE_EXT ||
            ttype == ERL_LARGE_TUPLE_EXT) {
        // handle complex type: e.g. list, table, dict
        if(tsize <= 0 || tsize > 3) {
            return -1;
        }
        // The first atom in the tuple defines the container type
        EI(ei_decode_tuple_header(buff, ti, &tsize));
        EI(ei_get_type(buff, ti, &ttype, &tsize));
        // ti now pointing at the first element of the tuple.
        // ttype has the type of the first element of the tuple.
        // tsize has the length of the first element of the tuple
        // upon return from this function, ti will point to the
        // second element in the tuple
    }

    if(ttype != ERL_ATOM_EXT) {
        return -1;
    }

    EI(ei_get_k_type_from_atom(buff, ti, tsize, type));
    return 0;
}

int ei_get_k_type_from_atom(char* buff, int* ti, int atom_size, int* type) {
    char* atom = malloc(sizeof(char)*(atom_size+1));
    atom[atom_size] = '\0';
    EIC(ei_decode_atom(buff, ti, atom), free(atom));
    EIC(get_type_identifier_from_string(atom, type), free(atom));;
    free(atom);
    return 0;
}

int get_type_identifier_from_string(const char* str, int *type) {
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
    } else {
        return -1;
    }
    return 0;
}

int ei_decode_types_values_tuple(char *buff, int *index, int *types_index, int *values_index) {
    int arity = 0;
    EI(ei_decode_tuple_header(buff, index, &arity));
    if(arity != 2) {
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
    EI(ei_get_k_type(buff, types_index, values_index, &ktype, &esize));
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
           *k = kp(0);
           EI(ei_decode_kj(buff, values_index, &(*k)->j, opts));
           break;
       case -KC: // char
           *k = kc(0);
           EI(ei_decode_kc(buff, values_index, &(*k)->u, opts));
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
           char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(buff, values_index, &s, &len));
           *k = ktn(KC, len);
           int i;
           for(i=0; i<len; ++i) {
               kC(*k)[i] = s[i];
           }
           free(s);
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
       default:
           return -1;
    }
    return 0;
}

// decoders
int ei_decode_ks(char* b, int* i, K* k, QOpts* opts) {
    char* s = NULL;
    int len = 0;
    EI(ei_decode_alloc_string(b, i, &s, &len));
    *k = ks(s);
    free(s);
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
    EI(ei_decode_double(b, i, kf));
    return 0;
}

int ei_decode_ke(char* b, int* i, float* ke, QOpts* opts) {
    double v = 0;
    EI(ei_decode_double(b, i, &v));
    *ke = (float)v;
    return 0;
}

int ei_decode_kc(char* b, int* i, char* kc, QOpts* opts) {
    EI(ei_decode_char(b, i, kc));
    return 0;
}

int ei_decode_kg(char* b, int* i, unsigned char* kg, QOpts* opts) {
    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *kg = (unsigned char)v;
    return 0;
}

int ei_decode_kb(char* b, int* i, unsigned char* kb, QOpts* opts) {
    EI(ei_decode_kg(b, i, kb, opts));
    return 0;
}

int ei_decode_datetime(char* b, int* i, double* dt, QOpts* opts) {
    if(opts->unix_timestamp_is_q_datetime) {
        long long v = 0;
        EI(ei_decode_longlong(b, i, &v));
        *dt = unix_timestamp_to_datetime(v);
    } else {
        EI(ei_decode_double(b, i, dt));
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
        EI(ei_get_k_type(b, ti, vi, &ktype, &esize));
        EI(ei_decode_same_list(b, vi, ktype, k, opts));
        return 0;
    } else if(ttype == ERL_LIST_EXT) {
        int vsize = 0;
        EI(ei_decode_list_header(b, ti, &tsize));
        EI(ei_decode_list_header(b, vi, &vsize));
        if(tsize != vsize) {
            return -1;
        }
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
    return -1;
}

int ei_decode_same_list(char* b, int* i, int ktype, K* k, QOpts* opts) {
    int arity = 0;
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
       {
           char c = 0;
           EI(ei_decode_kc(b, i, &c, opts));
           kC(*k)[list_index] = (unsigned char)c;
           break;
       }
       case -KE: // real
           EI(ei_decode_ke(b, i, &kE(*k)[list_index], opts));
           break;
       case -KH: // short
           EI(ei_decode_kh(b, i, &kH(*k)[list_index], opts));
           break;
       case -KG: // byte
       case -KB: // boolean
           EI(ei_decode_kg(b, i, &kG(*k)[list_index], opts));
           break;

       case -KS: // symbol
       {
           char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(b, i, &s, &len));
           kS(*k)[list_index] = ss(s);
           free(s);
           break;
       }
       case K_STR:
       {
           char* s = NULL;
           int len = 0;
           EI(ei_decode_alloc_string(b, i, &s, &len));
           K kstr = ktn(KC, len);
           int i;
           for(i=0; i<len; ++i) {
               kC(kstr)[i] = s[i];
           }
           kK(*k)[list_index] = kstr;
           free(s);
           break;
       }
       case 0:   // list
       case XT:  // table
       case XD:  // dict/keyed table
            // This function does not support these types because lists of
            // tables and dicts are always general lists
       default:
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
        EIC(ei_decode_and_assign(b, vi, KS, &r_cols, list_index, opts), r0(r_cols));
    }
    if(nvcols > 0) {
        EIC(ei_skip_term(b, vi), r0(r_cols)); // skip tail
    }

    //         ti                         vi
    //         |                          |
    //         v                          v
    // {table, {list, QTypes}}, {ColNames,ColValues}

    // decode column values
    EIC(ei_decode_list_header(b, vi, &nvcols), r0(r_cols));

    K r_vals = ktn(0, nvcols);
    for(list_index=0; list_index < nvcols; ++list_index) {
        K column = 0;
        EIC(ei_decode_k(b, ti, vi, &column, opts),
                safe_deref_list2(r_cols, KS, 0, 0, r_vals, 0, list_index, nvcols));
        kK(r_vals)[list_index] = column;
    }
    if(nvcols > 0) {
        EIC(ei_skip_term(b, vi), r02(r_cols, r_vals)); // skip tail
    }

    *k = xT(xD(r_cols, r_vals));
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
int is_infinity(char* atom) {
    return is_exact_atom(atom, "infinity");
}

int is_null(char* atom) {
    return is_exact_atom(atom, "null");
}

int is_exact_atom(char* atom, const char* compare) {
    int result = 0==strcmp(atom,compare);
    return result;
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

