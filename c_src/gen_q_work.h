#ifndef GEN_Q_WORK_H
#define GEN_Q_WORK_H

#include "erl_driver.h"
#include "ei.h"

#define FUNC_Q_H_OPEN 1
#define FUNC_Q_H_CLOSE 2
#define FUNC_Q_APPLY 3

typedef struct {
    int version;
    long op;
    void* data;
} QWork;

typedef struct {
    // input
    char* host;
    int hostlen;
    long port;
    char* userpass;
    int userpasslen;
    long timeout;

    // output
    int handle;
} QWorkHOpen;

typedef struct {
    long handle;
} QWorkHClose;

typedef struct {
} QWorkApply;

extern void genq_work(void *work);
extern void* genq_malloc_work(char *buff, ErlDrvSizeT bufflen);
extern void genq_free_work(void *work);

extern int genq_work_result(void *work, ei_x_buff *buff);

#endif
