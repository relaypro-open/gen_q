#include "gen_q_work.h"
#include <unistd.h>
#include <stdio.h>
#include "ei.h"
#include "ei_util.h"
#include "k.h"
#include "gen_q_log.h"
#include "q.h"
#include "e2q.h"
#include <string.h>

/* ALLOC internal function declarations */
int decode_op(char *buff, int* index, QWork *work);
int decode_op_hopen(char *buff, int *index, QWork *work);
int decode_op_hclose(char *buff, int *index, QWork *work);
int decode_op_apply(char *buff, int *index, QWork *work);

/* DO WORK internal function declarations */
void work_hopen(QWorkHOpen* data);
void work_hclose(QWorkHClose* data);
void work_apply(QWorkApply* data);

/* RESULT internal function declarations */
int work_result_hopen(QWorkHOpen* data, ei_x_buff *buff);
int work_result_hclose(QWorkHClose* data, ei_x_buff *buff);
int work_result_apply(QWorkApply* data, ei_x_buff *buff);

/* FREE internal function declarations */
void free_qwork_data(int op, void *data);
void free_qwork_hopen(QWorkHOpen *data);
void free_qwork_hclose(QWorkHClose *data);
void free_qwork_apply(QWorkApply *data);

#define HANDLE_DATA_ERROR                           \
    if(data == NULL) {                              \
        LOG("work data is null! %d\n", 0);          \
    }                                               \
    if(data->error != NULL) {                       \
        LOG("work data encoding error! %d\n", 0);   \
        EI(ei_x_encode_error_tuple_string_len(buff, \
                    data->error, data->errorlen));  \
        return 0;                                   \
    }

/**
 * ALLOC
 */
void* genq_malloc_work(char *buff, ErlDrvSizeT bufflen) {
    QWork *work = malloc(sizeof(QWork));
    work->op = -1;

    int index = 0;
    ei_decode_version(buff, &index, &work->version);

    LOG("malloc work version %d\n", work->version);

    int arity = 0;
    ei_decode_tuple_header(buff, &index, &arity);
    LOG("malloc work arity %d\n", arity);
    if(arity != 2) {
        return work;
    }

    ei_decode_long(buff, &index, &work->op);
    LOG("malloc work op %ld\n", work->op);

    int res = decode_op(buff, &index, work);
    if(res < 0) {
        return work;
    }

    return work;
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

    // init flags for safe frees
    data->hostlen = -1;
    data->userpasslen = -1;
    data->errorlen = -1;

    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op hopen arity %d\n", arity);
    if(arity != 4) {
        return -1;
    }

    // inputs
    EI(ei_decode_alloc_string(buff, index, &data->host, &data->hostlen));
    LOG("decode op hopen host %s\n", data->host);
    EI(ei_decode_long(buff, index, &data->port));
    LOG("decode op hopen port %ld\n", data->port);
    EI(ei_decode_alloc_string(buff, index, &data->userpass, &data->userpasslen));
    LOG("decode op hopen userpass %s\n", data->userpass);
    EI(ei_decode_long(buff, index, &data->timeout));
    LOG("decode op hopen timeout %ld\n", data->timeout);
    EI(ei_skip_term(buff, index)); // skip tail

    // outputs
    data->handle = -1;
    data->error = NULL;

    work->data = data;
    return 0;
}

int decode_op_hclose(char *buff, int* index, QWork* work) {
    QWorkHClose* data = malloc(sizeof(QWorkHClose));

    // init flags for safe frees
    data->errorlen = -1;

    static const int HCLOSE_ARGV = 1;
    int arity = 0;
    int type = 0;
    EI(ei_get_type(buff, index, &type, &arity));
    LOG("decode op hclose arity %d\n", arity);
    if(arity != HCLOSE_ARGV) {
        return -1;
    }

    // inputs
    if(type == ERL_STRING_EXT) {
        char argstr[HCLOSE_ARGV+1];
        EI(ei_decode_string(buff, index, argstr));
        data->handle = argstr[0];
    } else if(type == ERL_LIST_EXT) {
        EI(ei_decode_list_header(buff, index, &arity));
        EI(ei_decode_long(buff, index, &data->handle));
        EI(ei_skip_term(buff, index)); // skip tail
    } else {
        return -1;
    }

    LOG("decode op hclose handle %ld\n", data->handle);

    // outputs
    data->error = NULL;

    work->data = data;
    return 0;
}

int decode_op_apply(char *buff, int* index, QWork* work) {
    QWorkApply* data = malloc(sizeof(QWorkApply));

    // init flags for safe frees
    data->funclen = -1;
    data->bufflen = -1;
    data->has_x = 0;
    data->errorlen = -1;

    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op apply arity %d\n", arity);
    if(arity != 3) {
        return -1;
    }

    // inputs
    EI(ei_decode_long(buff, index, &data->handle));
    LOG("decode op apply handle %ld\n", data->handle);

    EI(ei_decode_alloc_string(buff, index, &data->func, &data->funclen));
    LOG("decode op apply func %s\n", data->func);

    int types_index = 0;
    int values_index = 0;
    EI(ei_decode_types_values_tuple(buff, index,
                &types_index, &values_index));
    LOG("decode op apply types_index %d\n", types_index);
    LOG("decode op apply values_index %d\n", values_index);
    data->bufflen = *index - types_index;
    LOG("decode op apply bufflen %d\n", data->bufflen);
    data->buff = malloc((sizeof(char))*data->bufflen);
    memcpy(data->buff, buff+types_index, data->bufflen);
    data->types_index = 0;
    data->values_index = data->types_index + (values_index - types_index);
    EI(ei_skip_term(buff, index)); // skip tail

    // outputs
    data->error = NULL;

    work->data = data;
    return 0;
}

/**
 * DO WORK
 */
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

void work_hopen(QWorkHOpen *data) {
    q_hopen(data);
}

void work_hclose(QWorkHClose *data) {
    q_hclose(data);
}

void work_apply(QWorkApply* data) {
    q_apply(data);
}

/**
 * RESULT
 */
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

int work_result_hopen(QWorkHOpen* data, ei_x_buff *buff) {
    LOG("work result hopen %d\n", 0);
    HANDLE_DATA_ERROR;
    if(data->handle < 0) {
        EI(ei_x_encode_error_tuple_string(buff, "Connection failed"));
        return 0;
    }
    EI(ei_x_encode_ok_tuple_header(buff));
    EI(ei_x_encode_long(buff, data->handle));
    return 0;
}

int work_result_hclose(QWorkHClose* data, ei_x_buff *buff) {
    LOG("work result hclose %d\n", 0);
    HANDLE_DATA_ERROR;
    EI(ei_x_encode_ok(buff));
    return 0;
}

int work_result_apply(QWorkApply* data, ei_x_buff *buff) {
    LOG("work result apply %d\n", 0);
    HANDLE_DATA_ERROR;
    if(!data->has_x) {
        LOG("work result no data %d\n", 0);
        EI(ei_x_encode_error_tuple_string(buff, "No data"));
        return 0;
    }
    LOG("work result encode ok tuple %d\n", 0);
    EI(ei_x_encode_ok_tuple_header(buff));
    LOG("work result append data %d\n", 0);
    EI(ei_x_append(buff, &data->x));
    //EI(ei_x_encode_atom(buff, "jms"));
    LOG("work result return %d\n", 0);
    return 0;
}

/**
 * FREE
 */
void genq_free_work(void *w) {
    LOG("free qwork %d\n", 0);
    QWork* work = (QWork*)w;
    free_qwork_data(work->op, work->data);
    free(work);
}

void free_qwork_data(int op, void *data) {
    switch(op) {
        case FUNC_Q_H_OPEN:
            LOG("free qwork hopen %d\n", 0);
            free_qwork_hopen((QWorkHOpen*)data);
            break;
        case FUNC_Q_H_CLOSE:
            LOG("free qwork hclose %d\n", 0);
            free_qwork_hclose((QWorkHClose*)data);
            break;
        case FUNC_Q_APPLY:
            LOG("free qwork apply %d\n", 0);
            free_qwork_apply((QWorkApply*)data);
            break;
    }
}

void free_qwork_hopen(QWorkHOpen *data) {
    if(data->errorlen >= 0) {
        LOG("free qwork hopen - errorlen %d\n", data->errorlen);
        free(data->error);
    }
    if(data->userpasslen >= 0) {
        LOG("free qwork hopen - userpasslen %d\n", data->userpasslen);
        free(data->userpass);
    }
    if(data->hostlen >= 0) {
        LOG("free qwork hopen - hostlen %d\n", data->hostlen);
        free(data->host);
    }
    free(data);
}

void free_qwork_hclose(QWorkHClose *data) {
    if(data->errorlen >= 0) {
        LOG("free qwork hclose - errorlen %d\n", data->errorlen);
        free(data->error);
    }
    free(data);
}

void free_qwork_apply(QWorkApply *data) {
    if(data->funclen >= 0) {
        LOG("free qwork apply - funclen %d\n", data->funclen);
        free(data->func);
    }
    if(data->bufflen >= 0) {
        LOG("free qwork apply - bufflen %d\n", data->bufflen);
        free(data->buff);
    }
    if(data->has_x) {
        LOG("free qwork apply - has_x %d\n", data->has_x);
        ei_x_free(&data->x);
    }
    if(data->errorlen >= 0) {
        LOG("free qwork apply - errorlen %d\n", data->errorlen);
        free(data->error);
    }
    free(data);
}
