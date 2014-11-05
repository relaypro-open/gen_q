#include "gen_q_work.h"
#include <unistd.h>
#include <stdio.h>
#include "ei.h"
#include "ei_util.h"
#include "k.h"
#include "gen_q_log.h"

int decode_op(char *buff, int* index, QWork *work);
int decode_op_hopen(char *buff, int *index, QWork *work);
int decode_op_hclose(char *buff, int *index, QWork *work);
int decode_op_apply(char *buff, int *index, QWork *work);

void free_qwork_data(int op, void *data);
void free_qwork_hopen(QWorkHOpen *data);
void free_qwork_hclose(QWorkHClose *data);
void free_qwork_apply(QWorkApply *data);

void work_hopen(QWorkHOpen* data);
void work_hclose(QWorkHClose* data);
void work_apply(QWorkApply* data);

int work_result_hopen(QWorkHOpen* data, ei_x_buff *buff);
int work_result_hclose(QWorkHClose* data, ei_x_buff *buff);
int work_result_apply(QWorkApply* data, ei_x_buff *buff);

void genq_work(void *w) {
    QWork *work = (QWork*)w;
    switch(work->op) {
        case FUNC_Q_H_OPEN:
            work_hopen((QWorkHOpen*)work->data);
            break;
        case FUNC_Q_H_CLOSE:
            work_hclose((QWorkHClose*)work->data);
            break;
        case FUNC_Q_APPLY:
            work_apply((QWorkApply*)work->data);
            break;
    }
}

int genq_work_result(void *w, ei_x_buff *buff) {
    QWork *work = (QWork*)w;
    switch(work->op) {
        case FUNC_Q_H_OPEN:
            return work_result_hopen((QWorkHOpen*)work->data, buff);
        case FUNC_Q_H_CLOSE:
            return work_result_hclose((QWorkHClose*)work->data, buff);
        case FUNC_Q_APPLY:
            return work_result_apply((QWorkApply*)work->data, buff);
    }
    return -1;
}

void work_hopen(QWorkHOpen *data) {
    // TODO ERRNO
    LOG("khpun %s %ld %s %ld\n", data->host, data->port, data->userpass, data->timeout);
    data->handle = khpun(data->host, data->port, data->userpass, data->timeout);
    LOG("khpun result %d\n", data->handle);
}

void work_hclose(QWorkHClose *data) {
    LOG("kclose %ld\n", data->handle);
    kclose(data->handle);
}

void work_apply(QWorkApply* data) {
    // TODO
}

int work_result_hopen(QWorkHOpen* data, ei_x_buff *buff) {
    // TODO handle error
    EI(ei_x_encode_ok_tuple_header(buff));
    EI(ei_x_encode_long(buff, data->handle));
    return 0;
}

int work_result_hclose(QWorkHClose* data, ei_x_buff *buff) {
    return -1;
}

int work_result_apply(QWorkApply* data, ei_x_buff *buff) {
    return -1;
}

void* genq_malloc_work(char *buff, ErlDrvSizeT bufflen) {
    QWork *work = malloc(sizeof(QWork));

    int index = 0;
    ei_decode_version(buff, &index, &work->version);

    LOG("malloc work version %d\n", work->version);

    int arity = 0;
    ei_decode_tuple_header(buff, &index, &arity);
    LOG("malloc work arity %d\n", arity);
    if(arity != 2) {
        return NULL;
    }

    ei_decode_long(buff, &index, &work->op);
    LOG("malloc work op %ld\n", work->op);

    int res = decode_op(buff, &index, work);
    if(res < 0) {
        return NULL;
    }

    return work;
}

void genq_free_work(void *w) {
    QWork* work = (QWork*)w;
    free_qwork_data(work->op, work->data);
    free(work);
}

void free_qwork_data(int op, void *data) {
    switch(op) {
        case FUNC_Q_H_OPEN:
            free_qwork_hopen((QWorkHOpen*)data);
            break;
        case FUNC_Q_H_CLOSE:
            free_qwork_hclose((QWorkHClose*)data);
            break;
        case FUNC_Q_APPLY:
            free_qwork_apply((QWorkApply*)data);
            break;
    }
}

void free_qwork_hopen(QWorkHOpen *data) {
    free(data->userpass);
    free(data->host);
    free(data->userpass);
    free(data);
}

void free_qwork_hclose(QWorkHClose *data) {
    free(data);
}

void free_qwork_apply(QWorkApply *data) {
    free(data);
}

int decode_op(char *buff, int* index, QWork *work) {
    switch(work->op) {
        case FUNC_Q_H_OPEN:
            return decode_op_hopen(buff, index, work);
        case FUNC_Q_H_CLOSE:
            return decode_op_hclose(buff, index, work);
        case FUNC_Q_APPLY:
            return decode_op_apply(buff, index, work);
    }
    return -1;
}

int decode_op_hopen(char *buff, int* index, QWork* work) {
    QWorkHOpen* data = malloc(sizeof(QWorkHOpen));
    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op hopen arity %d\n", arity);
    if(arity != 4) {
        return -1;
    }
    EI(ei_decode_alloc_string(buff, index, &data->host, &data->hostlen));
    LOG("decode op hopen host %s\n", data->host);
    EI(ei_decode_long(buff, index, &data->port));
    LOG("decode op hopen port %ld\n", data->port);
    EI(ei_decode_alloc_string(buff, index, &data->userpass, &data->userpasslen));
    LOG("decode op hopen userpass %s\n", data->userpass);
    EI(ei_decode_long(buff, index, &data->timeout));
    LOG("decode op hopen timeout %ld\n", data->timeout);

    work->data = data;
    return 0;
}

int decode_op_hclose(char *buff, int* index, QWork* work) {
    QWorkHClose* data = malloc(sizeof(QWorkHClose));
    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op hclose arity %d\n", arity);
    if(arity != 1) {
        return -1;
    }
    EI(ei_decode_long(buff, index, &data->handle));
    LOG("decode op hclose handle %ld\n", data->handle);
    work->data = data;
    return 0;
}

int decode_op_apply(char *buff, int* index, QWork* work) {
    QWorkApply* data = malloc(sizeof(QWorkApply));
    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op apply arity %d\n", arity);
    if(arity != 3) {
        return -1;
    }
    // TODO - decode types and values
    work->data = data;
    return 0;
}
