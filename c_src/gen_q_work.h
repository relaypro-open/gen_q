#ifndef GEN_Q_WORK_H
#define GEN_Q_WORK_H

#include "erl_driver.h"
#include "ei.h"
#include "k.h"

#define FUNC_OPTS 0
#define FUNC_Q_H_OPEN 1
#define FUNC_Q_H_CLOSE 2
#define FUNC_Q_APPLY 3
#define FUNC_Q_H_KILL 4
#define FUNC_Q_DECODE_BINARY 5
#define FUNC_Q_DBOPEN 6
#define FUNC_Q_DBNEXT 7
#define FUNC_Q_DBCLOSE 8

typedef struct {
    int unix_timestamp_is_q_datetime;
    int day_seconds_is_q_time;
} QOpts;

typedef struct {
    int version;
    long op;
    void* data;
    unsigned int* dispatch_key;
    QOpts* opts;
} QWork;

typedef struct {
    // input
    unsigned char* host;
    int hostlen;
    long port;
    unsigned char* userpass;
    int userpasslen;
    long timeout;

    // output
    int handle;
    int errorlen;
    char* error;
} QWorkHOpen;

typedef struct {
    // input
    long handle;

    // output
    int errorlen;
    char* error;
} QWorkHClose;

typedef struct {
    // input
    long handle;

    // output
    int errorlen;
    char* error;
} QWorkHKill;

typedef struct {
    // input
    K binary;

    // output
    int errorlen;
    char* error;
    int has_x;
    ei_x_buff x;
} QWorkDecodeBinary;

typedef struct {
    // input
    long handle;
    int funclen;
    unsigned char* func;
    int bufflen;
    char* buff;
    int types_index;
    int values_index;

    // output
    int errorlen;
    char* error;
    int has_x;
    ei_x_buff x;
} QWorkApply;

typedef struct {
    // input
    long n;
    int bufflen;
    char* buff;
    int types_index;
    int values_index;

    // output
    int errorlen;
    char* error;
    int has_x;
    ei_x_buff x;
} QWorkDbOp;

extern void copy_qopts(QOpts* src, QOpts* dest);

/**
 * ALLOC
 */
extern void* genq_alloc_work(char *buff, ErlDrvSizeT bufflen);

/**
 * DO WORK
 */
extern void genq_work(void *work);

/**
 * RESULT
 */
extern int genq_work_result(void *work, ei_x_buff *buff);

/**
 * FREE
 */
extern void genq_free_work(void *work);

#endif
