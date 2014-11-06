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

// atom decoders
int ei_decode_ki(char* b, int* i, int* ki);
int ei_decode_kj(char* b, int* i, long long* kj);
int ei_decode_kh(char* b, int* i, short* kh);
int ei_decode_kf(char* b, int* i, double* kf);
int ei_decode_ke(char* b, int* i, float* ke);
int ei_decode_kc(char* b, int* i, char* kc);
int ei_decode_kg(char* b, int* i, unsigned char* kg);
int ei_decode_kb(char* b, int* i, unsigned char* kb);

// helpers
int is_exact_atom(char* atom, const char* compare);
int is_infinity(char* atom);
int is_null(char* atom);

int ei_get_k_type(char* buff, int* ti, int* vi, int* type, int* size) {
    int ttype = 0; // erlang type of ktype element
    int tsize = 0; // erlang size of ktype element
    EI(ei_get_type(buff, ti, &ttype, &tsize));
    if(ttype == ERL_SMALL_TUPLE_EXT ||
            ttype == ERL_LARGE_TUPLE_EXT) {
        // handle complex type: e.g. list, table, dict
        return -1;
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

int ei_decode_k(char *buff, int *index, int types_index, int values_index, K* k) {
    int ktype = 0;
    int esize = 0;
    EI(ei_get_k_type(buff, &types_index, &values_index, &ktype, &esize));
    switch(ktype) {

       case -KT: // time
           *k = kt(0);
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KV: // second
           *k = ki(0);
           (*k)->t = -KV;
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KU: // minute
           *k = ki(0);
           (*k)->t = -KU;
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KN: // timespan
           *k = kj(0);
           (*k)->t = -KN;
           EI(ei_decode_kj(buff, &values_index, &(*k)->j));
           break;
       case -KZ: // datetime
           *k = kz(0);
           EI(ei_decode_kf(buff, &values_index, &(*k)->f));
           break;
       case -KD: // date
           *k = kd(0);
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KM: // month
           *k = ki(0);
           (*k)->t = -KM;
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KP: // timestamp
           *k = kp(0);
           EI(ei_decode_kj(buff, &values_index, &(*k)->j));
           break;
       case -KC: // char
           *k = kc(0);
           EI(ei_decode_kc(buff, &values_index, &(*k)->u));
           break;
       case -KF: // float
           *k = kf(0);
           EI(ei_decode_kf(buff, &values_index, &(*k)->f));
           break;
       case -KE: // real
           *k = ke(0);
           EI(ei_decode_ke(buff, &values_index, &(*k)->e));
           break;
       case -KJ: // long
           *k = kj(0);
           EI(ei_decode_kj(buff, &values_index, &(*k)->j));
           break;
       case -KI: // int
           *k = ki(0);
           EI(ei_decode_ki(buff, &values_index, &(*k)->i));
           break;
       case -KH: // short
           *k = kh(0);
           EI(ei_decode_kh(buff, &values_index, &(*k)->h));
           break;
       case -KG: // byte
           *k = kg(0);
           EI(ei_decode_kg(buff, &values_index, &(*k)->g));
           break;
       case -KB: // boolean
           *k = kb(0);
           EI(ei_decode_kb(buff, &values_index, &(*k)->g));
           break;

       case -KS: // symbol
       case 0:   // any list - this differs from normal q type checking
       case K_STR:
       case XT:  // table
       case XD:  // dict/keyed table
       default:
           return -1;
    }
    return 0;
}

int ei_decode_ki(char* b, int* i, int* ki) {
    EI_NULL_OR_INFINITY(ki, ni, wi);

    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *ki = (int)v;
    return 0;
}

int ei_decode_kj(char* b, int* i, long long* kj) {
    EI_NULL_OR_INFINITY(kj, nj, wj);

    EI(ei_decode_longlong(b, i, kj));
    return 0;
}

int ei_decode_kh(char* b, int* i, short* kh) {
    EI_NULL_OR_INFINITY(kh, nh, wh);

    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *kh = (short)v;
    return 0;
}

int ei_decode_kf(char* b, int* i, double* kf) {
    EI_NULL_OR_INFINITY(kf, nf, wf);
    EI(ei_decode_double(b, i, kf));
    return 0;
}

int ei_decode_ke(char* b, int* i, float* ke) {
    double v = 0;
    EI(ei_decode_double(b, i, &v));
    *ke = (float)v;
    return 0;
}

int ei_decode_kc(char* b, int* i, char* kc) {
    EI(ei_decode_char(b, i, kc));
    return 0;
}

int ei_decode_kg(char* b, int* i, unsigned char* kg) {
    long v = 0;
    EI(ei_decode_long(b, i, &v));
    *kg = (unsigned char)v;
    return 0;
}

int ei_decode_kb(char* b, int* i, unsigned char* kb) {
    EI(ei_decode_kg(b, i, kb));
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

