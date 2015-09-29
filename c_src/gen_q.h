#ifndef GEN_Q_H
#define GEN_Q_H

#include "erl_driver.h"

#if KXVER == 3
#define FMT_KN "%lld"
#else
#define FMT_KN "%d"
#endif

#define genq_alloc(X) driver_alloc(X)
#define genq_free(X) driver_free(X)

#endif
