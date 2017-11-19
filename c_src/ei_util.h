#ifndef EI_UTIL_H
#define EI_UTIL_H
#include "ei.h"
#include "gen_q_log.h"

#define EI(call)                      \
    do {                              \
        if((call) < 0) {              \
            LOG("ERROR call to EI func failed %s\n", #call); \
            return -1;                \
        }                             \
    } while(0)

#define EIC(call, cleanup)            \
    do {                              \
        if((call) < 0) {              \
            (cleanup);                \
            LOG("ERROR call to EI func failed %s\n", #call); \
            return -1;                \
        }                             \
    } while(0)

extern int ei_x_encode_ok(ei_x_buff *buff);
extern int ei_x_encode_ok_tuple_header(ei_x_buff *buff);
extern int ei_x_encode_ok_tuple_header_n(ei_x_buff *buff, int n);
extern int ei_x_encode_error_tuple_atom(ei_x_buff *buff, char *atom);
extern int ei_x_encode_error_tuple_string(ei_x_buff *buff, char *str);
extern int ei_x_encode_error_tuple_atom_len(ei_x_buff *buff, char *atom, int atomlen);
extern int ei_x_encode_error_tuple_string_len(ei_x_buff *buff, char *str, int strlen);

extern int ei_decode_alloc_string(char *buff, int *index, unsigned char **str, int *len);

extern int ei_decode_atom_safe(char *buff, int *index, unsigned char *a);
extern int ei_decode_char_safe(char *buff, int *index, unsigned char *c);
extern int ei_decode_string_safe(char *buff, int *index, unsigned char *p);

#endif
