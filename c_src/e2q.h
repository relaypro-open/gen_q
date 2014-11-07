#ifndef E2Q_H
#define E2Q_H

#include "k.h"
#include "gen_q_work.h"

extern int ei_decode_types_values_tuple(char *buff, int *index, int *types_index, int *values_index);
extern int ei_decode_k(char *buff, int* types_index, int* values_index, K* k, QOpts* opts);

#endif
