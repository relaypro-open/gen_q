#ifndef E2Q_H
#define E2Q_H

#include "k.h"

extern int ei_decode_types_values_tuple(char *buff, int *index, int *types_index, int *values_index);

extern K e2q(char *buff, int *index, int types_index, int values_index);

#endif
