#ifndef GEN_Q_WORK_H
#define GEN_Q_WORK_H

#include "erl_driver.h"

typedef struct {
    int version;
    long timeout;
} QWork;

extern void genq_work(void *data);
extern void* genq_malloc_work(char *buff, ErlDrvSizeT bufflen);
extern void genq_free_work(void *work);

#endif
