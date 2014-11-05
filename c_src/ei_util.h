#ifndef EI_UTIL_H
#define EI_UTIL_H
#include "ei.h"

#define EI(call)                      \
    do {                              \
        if((call) < 0) { return -1; } \
    } while(0)

extern int ei_x_encode_ok(ei_x_buff *buff);
extern int ei_x_encode_ok_tuple_header(ei_x_buff *buff);
extern int ei_x_encode_error_tuple_atom(ei_x_buff *buff, char *atom);
extern int ei_x_encode_error_tuple_string(ei_x_buff *buff, char *str);
extern int ei_x_encode_error_tuple_atom_len(ei_x_buff *buff, char *atom, int atomlen);
extern int ei_x_encode_error_tuple_string_len(ei_x_buff *buff, char *str, int strlen);

extern int ei_decode_alloc_string(char *buff, int *index, char **str, int *len);

#endif
