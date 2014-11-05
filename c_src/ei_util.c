#include "ei_util.h"

int ei_x_encode_ok(ei_x_buff *buff) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_atom(buff, "ok"));
    return 0;
}

int ei_x_encode_ok_tuple_header(ei_x_buff *buff) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_tuple_header(buff, 2));
    EI_X_ENC(ei_x_encode_atom(buff, "ok"));
    return 0;
}

int ei_x_encode_error_tuple_atom(ei_x_buff *buff, char *atom) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_tuple_header(buff, 2));
    EI_X_ENC(ei_x_encode_atom(buff, "error"));
    EI_X_ENC(ei_x_encode_atom(buff, atom));
    return 0;
}

int ei_x_encode_error_tuple_string(ei_x_buff *buff, char *str) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_tuple_header(buff, 2));
    EI_X_ENC(ei_x_encode_atom(buff, "error"));
    EI_X_ENC(ei_x_encode_string(buff, str));
    return 0;
}

int ei_x_encode_error_tuple_atom_len(ei_x_buff *buff, char *atom, int atomlen) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_tuple_header(buff, 2));
    EI_X_ENC(ei_x_encode_atom(buff, "error"));
    EI_X_ENC(ei_x_encode_atom_len(buff, atom, atomlen));
    return 0;
}

int ei_x_encode_error_tuple_string_len(ei_x_buff *buff, char *str, int strlen) {
    EI_X_ENC(ei_x_encode_version(buff));
    EI_X_ENC(ei_x_encode_tuple_header(buff, 2));
    EI_X_ENC(ei_x_encode_atom(buff, "error"));
    EI_X_ENC(ei_x_encode_string_len(buff, str, strlen));
    return 0;
}
