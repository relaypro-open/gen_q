#include "ei_util.h"
#include <stdlib.h>

int ei_x_encode_ok(ei_x_buff *buff) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_atom(buff, "ok"));
    return 0;
}

int ei_x_encode_ok_tuple_header(ei_x_buff *buff) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_tuple_header(buff, 2));
    EI(ei_x_encode_atom(buff, "ok"));
    return 0;
}

int ei_x_encode_error_tuple_atom(ei_x_buff *buff, char *atom) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_tuple_header(buff, 2));
    EI(ei_x_encode_atom(buff, "error"));
    EI(ei_x_encode_atom(buff, atom));
    return 0;
}

int ei_x_encode_error_tuple_string(ei_x_buff *buff, char *str) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_tuple_header(buff, 2));
    EI(ei_x_encode_atom(buff, "error"));
    EI(ei_x_encode_string(buff, str));
    return 0;
}

int ei_x_encode_error_tuple_atom_len(ei_x_buff *buff, char *atom, int atomlen) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_tuple_header(buff, 2));
    EI(ei_x_encode_atom(buff, "error"));
    EI(ei_x_encode_atom_len(buff, atom, atomlen));
    return 0;
}

int ei_x_encode_error_tuple_string_len(ei_x_buff *buff, char *str, int strlen) {
    EI(ei_x_encode_version(buff));
    EI(ei_x_encode_tuple_header(buff, 2));
    EI(ei_x_encode_atom(buff, "error"));
    EI(ei_x_encode_string_len(buff, str, strlen));
    return 0;
}

int ei_decode_alloc_string(char *buff, int *index, char **str, int *len) {
    int type = 0;
    EI(ei_get_type(buff, index, &type, len));
    if(type == ERL_STRING_EXT) {
        *str = malloc((sizeof(char))*(*len+1));
        (*str)[*len] = '\0';
        EIC(ei_decode_string(buff, index, *str), free(str));
        return 0;
    } else if(type == ERL_LIST_EXT) {
        // String larger than 65535
        int arity = 0;
        EI(ei_decode_list_header(buff, index, &arity));
        *str = malloc((sizeof(char))*(*len+1));
        int i;
        for(i=0; i < *len; ++i) {
            EIC(ei_decode_char(buff, index, &(*str)[i]), free(str));
        }
        (*str)[*len] = '\0';
        EIC(ei_skip_term(buff, index), free(str)); // skip tail

        return 0;
    } else if(type == ERL_ATOM_EXT) {
        *str = malloc(sizeof(char)*(*len+1));
        (*str)[*len] = '\0';
        EIC(ei_decode_atom(buff, index, *str), free(str));
        return 0;
    } else if(type == ERL_BINARY_EXT) {
        *str = malloc(sizeof(char)*(*len+1));
        (*str)[*len] = '\0';
        EIC(ei_decode_binary(buff, index, *str, len), free(str));
        return 0;
    } else {
        return -1;
    }
}
