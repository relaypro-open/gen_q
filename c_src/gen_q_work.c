#include "gen_q_work.h"
#include "gen_q.h"
#include <unistd.h>
#include <stdio.h>
#include "ei.h"
#include "ei_util.h"
#include "k.h"
#include "gen_q_log.h"
#include "q.h"
#include "e2q.h"
#include <string.h>
#include <sys/types.h>
#include <sys/syscall.h>

/* ALLOC internal function declarations */
int decode_op(char *buff, int* index, QWork *work);
int decode_op_opts(char *buff, int *index, QWork *work);
int decode_op_hopen(char *buff, int *index, QWork *work);
int decode_op_hclose(char *buff, int *index, QWork *work);
int decode_op_apply(char *buff, int *index, QWork *work);
int decode_op_hkill(char *buff, int *index, QWork *work);
int decode_op_decodebinary(char *buff, int *index, QWork *work);
int decode_op_dbop(char *buff, int *index, QWork *work);

/* DO WORK internal function declarations */
void work_hopen(QWorkHOpen* data);
void work_hclose(QWorkHClose* data);
void work_apply(QWorkApply* data, QOpts* opts);
void work_hkill(QWorkHKill* data);
void work_decodebinary(QWorkDecodeBinary* data, QOpts* opts);

/* RESULT internal function declarations */
int work_result_hopen(QWorkHOpen* data, ei_x_buff *buff);
int work_result_hclose(QWorkHClose* data, ei_x_buff *buff);
int work_result_apply(QWorkApply* data, ei_x_buff *buff);
int work_result_hkill(QWorkHKill* data, ei_x_buff *buff);
int work_result_decodebinary(QWorkDecodeBinary* data, ei_x_buff *buff);
int work_result_dbop(QWorkDbOp* data, ei_x_buff *buff);

/* FREE internal function declarations */
void free_qopts(QOpts* data);
void free_qwork_data(int op, void *data);
void free_qwork_hopen(QWorkHOpen *data);
void free_qwork_hclose(QWorkHClose *data);
void free_qwork_apply(QWorkApply *data);
void free_qwork_hkill(QWorkHKill* data);
void free_qwork_decodebinary(QWorkDecodeBinary* data);
void free_qwork_dbop(QWorkDbOp* data);

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

void copy_qopts(QOpts* src, QOpts* dest) {
    dest->unix_timestamp_is_q_datetime = src->unix_timestamp_is_q_datetime;
    dest->day_seconds_is_q_time = src->day_seconds_is_q_time;
}

/**
 * ALLOC
 */
void* genq_alloc_work(char *buff, ErlDrvSizeT bufflen) {
    QWork *work = genq_alloc(sizeof(QWork));
    work->op = -1;
    work->opts = 0;
    work->data = NULL;
    work->dispatch_key = NULL;

    int index = 0;
    ei_decode_version(buff, &index, &work->version);

    LOG("alloc work version %d\n", work->version);

    int arity = 0;
    ei_decode_tuple_header(buff, &index, &arity);

    {
        int etype = 0;
        int atom_size = 0;
        ei_get_type(buff, &index, &etype, &atom_size);
        if(etype == ERL_ATOM_EXT) {
            ei_skip_term(buff, &index);
        } else {
            long dispatch_key = 0;
            ei_decode_long(buff, &index, &dispatch_key);
            work->dispatch_key = genq_alloc(sizeof(unsigned int));
            *(work->dispatch_key) = (unsigned int)dispatch_key;
        }
    }

    arity = 0;
    ei_decode_tuple_header(buff, &index, &arity);
    LOG("alloc work arity %d\n", arity);
    if(arity != 2) {
        return work;
    }

    ei_decode_long(buff, &index, &work->op);
    LOG("alloc work op %ld\n", work->op);

    int res = decode_op(buff, &index, work);
    if(res < 0) {
        free_qwork_data(work->op, work->data);
        work->data = NULL;
        return work;
    }

    return work;
}

int decode_op(char *buff, int* index, QWork *work) {
    switch(work->op) {
        case FUNC_OPTS:
            return decode_op_opts(buff, index, work);
        case FUNC_Q_H_OPEN:
            return decode_op_hopen(buff, index, work);
        case FUNC_Q_H_CLOSE:
            return decode_op_hclose(buff, index, work);
        case FUNC_Q_APPLY:
            return decode_op_apply(buff, index, work);
        case FUNC_Q_H_KILL:
            return decode_op_hkill(buff, index, work);
        case FUNC_Q_DECODE_BINARY:
            return decode_op_decodebinary(buff, index, work);
        case FUNC_Q_DBOPEN:
            return decode_op_dbop(buff, index, work);
        case FUNC_Q_DBNEXT:
            return decode_op_dbop(buff, index, work);
        case FUNC_Q_DBCLOSE:
            return decode_op_dbop(buff, index, work);
    }
    return -1;
}

#define ASSIGN_PROPERTY(key, prop)   \
    do {                             \
        if(0==strcmp(#key,(char*)prop)) { \
            data->key = 1;           \
        }                            \
    } while(0)

int decode_op_opts(char* buff, int* index, QWork* work) {
    QOpts* data = genq_alloc(sizeof(QOpts));
    work->data = data;

    // init defaults
    data->unix_timestamp_is_q_datetime = 0;
    data->day_seconds_is_q_time = 0;

    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op opts arity %d\n", arity);
    int i;
    for(i=0; i<arity; ++i) {
        unsigned char* property = 0;
        int propertylen = 0;
        EI(ei_decode_alloc_string(buff, index, &property, &propertylen));

        LOG("decode op opts - found property %s\n", property);

        // not the most elegant solution. to be revisited
        ASSIGN_PROPERTY(unix_timestamp_is_q_datetime, property);
        ASSIGN_PROPERTY(day_seconds_is_q_time, property);

        genq_free(property);
    }
    if(arity > 0) {
        EI(ei_skip_term(buff, index)); // skip tail
    }

    LOG("decode op opts utiqd=%d, dsiqt=%d\n", data->unix_timestamp_is_q_datetime, data->day_seconds_is_q_time);

    return 0;
}

int decode_op_hopen(char *buff, int* index, QWork* work) {
    QWorkHOpen* data = genq_alloc(sizeof(QWorkHOpen));
    work->data = data;

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

    if(arity > 0) {
        EI(ei_skip_term(buff, index)); // skip tail
    }

    // outputs
    data->handle = -1;
    data->error = NULL;

    return 0;
}

int decode_op_hclose(char *buff, int* index, QWork* work) {
    QWorkHClose* data = genq_alloc(sizeof(QWorkHClose));
    work->data = data;

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
    //
    // a list of a single int comes in as a string usually
    if(type == ERL_STRING_EXT) {
        unsigned char argstr[HCLOSE_ARGV+1];
        EI(ei_decode_string_safe(buff, index, argstr));
        data->handle = argstr[0];
    } else if(type == ERL_LIST_EXT) {
        EI(ei_decode_list_header(buff, index, &arity));
        EI(ei_decode_long(buff, index, &data->handle));
        if(arity > 0) {
            EI(ei_skip_term(buff, index)); // skip tail
        }
    } else {
        return -1;
    }

    LOG("decode op hclose handle %ld\n", data->handle);

    // outputs
    data->error = NULL;

    return 0;
}

int decode_op_apply(char *buff, int* index, QWork* work) {
    QWorkApply* data = genq_alloc(sizeof(QWorkApply));
    work->data = data;

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
    data->buff = genq_alloc((sizeof(char))*data->bufflen);
    memcpy(data->buff, buff+types_index, data->bufflen);
    data->types_index = 0;
    data->values_index = data->types_index + (values_index - types_index);

    if(arity > 0) {
        EI(ei_skip_term(buff, index)); // skip tail
    }

    // outputs
    data->error = NULL;

    return 0;
}

int decode_op_hkill(char *buff, int* index, QWork* work) {
    QWorkHKill* data = genq_alloc(sizeof(QWorkHKill));
    work->data = data;

    // init flags for safe frees
    data->errorlen = -1;

    static const int HKILL_ARGV = 1;
    int arity = 0;
    int type = 0;
    EI(ei_get_type(buff, index, &type, &arity));
    LOG("decode op hclose arity %d\n", arity);
    if(arity != HKILL_ARGV) {
        return -1;
    }

    // inputs
    if(type == ERL_STRING_EXT) {
        unsigned char argstr[HKILL_ARGV+1];
        EI(ei_decode_string_safe(buff, index, argstr));
        data->handle = argstr[0];
    } else if(type == ERL_LIST_EXT) {
        EI(ei_decode_list_header(buff, index, &arity));
        EI(ei_decode_long(buff, index, &data->handle));
        if(arity > 0) {
            EI(ei_skip_term(buff, index)); // skip tail
        }
    } else {
        return -1;
    }

    LOG("decode op hkill handle %ld\n", data->handle);

    // outputs
    data->error = NULL;

    return 0;
}

int decode_op_decodebinary(char *buff, int* index, QWork* work) {
    QWorkDecodeBinary* data = genq_alloc(sizeof(QWorkDecodeBinary));
    work->data = data;

    // init flags for safe frees
    data->errorlen = -1;
    data->has_x = 0;

    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    if(arity != 1) {
        LOG("ERROR too many inputs %d\n", arity);
        return -1;
    }

    int binarylen = 0;
    int type = 0;
    EI(ei_get_type(buff, index, &type, &binarylen));
    if(type != ERL_BINARY_EXT) {
        LOG("ERROR input is not a binary %d\n", type);
        return -1;
    }

    LOG("decodebinary type %d len %d\n", type, binarylen);

    data->binary = ktn(KG, binarylen);
    long v = 0;

    LOG("decodebinary decoding %d\n", binarylen);
    EI(ei_decode_binary(buff, index, kG(data->binary), &v));
    LOG("decodebinary got binary len %ld\n", v);

    EI(ei_skip_term(buff, index)); // skip tail

    // outputs
    data->error = NULL;
    data->has_x = 0;

    return 0;
}

int decode_op_dbop(char *buff, int* index, QWork* work) {
    QWorkDbOp* data = genq_alloc(sizeof(QWorkDbOp));
    work->data = data;

    // init flags for safe frees
    data->bufflen = -1;
    data->has_x = 0;
    data->errorlen = -1;
    data->handle = 0;

    int arity = 0;
    EI(ei_decode_list_header(buff, index, &arity));
    LOG("decode op dbop arity %d\n", arity);
    if(arity != 2) {
        return -1;
    }

    EI(ei_decode_long(buff, index, &data->n));

    // inputs
    int types_index = 0;
    int values_index = 0;
    EI(ei_decode_types_values_tuple(buff, index,
                &types_index, &values_index));
    LOG("decode op dbop types_index %d\n", types_index);
    LOG("decode op dbop values_index %d\n", values_index);
    data->bufflen = *index - types_index;
    LOG("decode op apply bufflen %d\n", data->bufflen);
    data->buff = genq_alloc((sizeof(char))*data->bufflen);
    memcpy(data->buff, buff+types_index, data->bufflen);
    data->types_index = 0;
    data->values_index = data->types_index + (values_index - types_index);

    if(arity > 0) {
        EI(ei_skip_term(buff, index)); // skip tail
    }

    // outputs
    data->error = NULL;

    return 0;

}

/**
 * DO WORK
 */
void genq_work(void *w) {
    QWork *work = (QWork*)w;
    switch(work->op) {
        case FUNC_OPTS:
            // no op
            break;
        case FUNC_Q_H_OPEN:
            work_hopen((QWorkHOpen*)work->data);
            break;
        case FUNC_Q_H_CLOSE:
            work_hclose((QWorkHClose*)work->data);
            break;
        case FUNC_Q_APPLY:
            work_apply((QWorkApply*)work->data, work->opts);
            break;
        case FUNC_Q_H_KILL:
            work_hkill((QWorkHKill*)work->data);
            break;
        case FUNC_Q_DECODE_BINARY:
            work_decodebinary((QWorkDecodeBinary*)work->data, work->opts);
            break;
        case FUNC_Q_DBOPEN:
            q_dbopen((QWorkDbOp*)work->data, work->opts);
            break;
        case FUNC_Q_DBNEXT:
            q_dbnext((QWorkDbOp*)work->data, work->opts);
            break;
        case FUNC_Q_DBCLOSE:
            q_dbclose((QWorkDbOp*)work->data, work->opts);
            break;
    }
}

void work_hopen(QWorkHOpen *data) {
    q_hopen(data);
}

void work_hclose(QWorkHClose *data) {
    q_hclose(data);
}

void work_apply(QWorkApply* data, QOpts* opts) {
    q_apply(data, opts);
}

void work_hkill(QWorkHKill* data) {
    q_hkill(data);
}

void work_decodebinary(QWorkDecodeBinary* data, QOpts* opts) {
    q_decodebinary(data, opts);
}

/**
 * RESULT
 */
int genq_work_result(void *w, ei_x_buff *buff) {
    QWork *work = (QWork*)w;
    switch(work->op) {
        case FUNC_OPTS:
            // no op
            return 0;
        case FUNC_Q_H_OPEN:
            return work_result_hopen((QWorkHOpen*)work->data, buff);
        case FUNC_Q_H_CLOSE:
            return work_result_hclose((QWorkHClose*)work->data, buff);
        case FUNC_Q_APPLY:
            return work_result_apply((QWorkApply*)work->data, buff);
        case FUNC_Q_H_KILL:
            return work_result_hkill((QWorkHKill*)work->data, buff);
        case FUNC_Q_DECODE_BINARY:
            return work_result_decodebinary((QWorkDecodeBinary*)work->data, buff);
        case FUNC_Q_DBOPEN:
            return work_result_dbop((QWorkDbOp*)work->data, buff);
        case FUNC_Q_DBNEXT:
            return work_result_dbop((QWorkDbOp*)work->data, buff);
        case FUNC_Q_DBCLOSE:
            return work_result_dbop((QWorkDbOp*)work->data, buff);
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
    LOG("work result return %d\n", 0);
    return 0;
}

int work_result_hkill(QWorkHKill* data, ei_x_buff* buff) {
    LOG("work result hkill %d\n", 0);
    HANDLE_DATA_ERROR;
    EI(ei_x_encode_ok(buff));
    return 0;
}

int work_result_decodebinary(QWorkDecodeBinary* data, ei_x_buff* buff) {
    HANDLE_DATA_ERROR;
    if(!data->has_x) {
        EI(ei_x_encode_error_tuple_string(buff, "No data"));
        return 0;
    }
    EI(ei_x_encode_ok_tuple_header(buff));
    EI(ei_x_append(buff, &data->x));
    return 0;
}

int work_result_dbop(QWorkDbOp *data, ei_x_buff* buff) {
    LOG("work result dbop %d\n", 0);
    HANDLE_DATA_ERROR;
    if(!data->has_x) {
        LOG("work result no data %d\n", 0);
        EI(ei_x_encode_error_tuple_string(buff, "No data"));
        return 0;
    }
    LOG("work result encode ok tuple %d\n", 0);
    EI(ei_x_encode_ok_tuple_header_n(buff, 3));
    LOG("work result append data %d\n", 0);

    EI(ei_x_encode_long(buff, data->handle));
    EI(ei_x_append(buff, &data->x));
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
    genq_free(work->dispatch_key);
    genq_free(work);
}

void free_qwork_data(int op, void *data) {
    if(!data) return;
    switch(op) {
        case FUNC_OPTS:
            LOG("free qwork opts %d\n", 0);
            free_qopts((QOpts*)data);
            break;
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
        case FUNC_Q_H_KILL:
            LOG("free qwork hkill %d\n", 0);
            free_qwork_hkill((QWorkHKill*)data);
            break;
        case FUNC_Q_DBOPEN:
            LOG("free qwork dbopen %d\n", 0);
            free_qwork_dbop((QWorkDbOp*)data);
            break;
        case FUNC_Q_DBNEXT:
            LOG("free qwork dbnext %d\n", 0);
            free_qwork_dbop((QWorkDbOp*)data);
            break;
        case FUNC_Q_DBCLOSE:
            LOG("free qwork dbclose %d\n", 0);
            free_qwork_dbop((QWorkDbOp*)data);
            break;

    }
}

void free_qopts(QOpts* data) {
    if(!data) return;
    genq_free(data);
}

void free_qwork_hopen(QWorkHOpen *data) {
    if(!data) return;
    if(data->errorlen >= 0) {
        LOG("free qwork hopen - errorlen %d\n", data->errorlen);
        genq_free(data->error);
    }
    if(data->userpasslen >= 0) {
        LOG("free qwork hopen - userpasslen %d\n", data->userpasslen);
        genq_free(data->userpass);
    }
    if(data->hostlen >= 0) {
        LOG("free qwork hopen - hostlen %d\n", data->hostlen);
        genq_free(data->host);
    }
    genq_free(data);
}

void free_qwork_hclose(QWorkHClose *data) {
    if(!data) return;
    if(data->errorlen >= 0) {
        LOG("free qwork hclose - errorlen %d\n", data->errorlen);
        genq_free(data->error);
    }
    genq_free(data);
}

void free_qwork_apply(QWorkApply *data) {
    if(!data) return;
    if(data->funclen >= 0) {
        LOG("free qwork apply - funclen %d\n", data->funclen);
        genq_free(data->func);
    }
    if(data->bufflen >= 0) {
        LOG("free qwork apply - bufflen %d\n", data->bufflen);
        genq_free(data->buff);
    }
    if(data->has_x) {
        LOG("free qwork apply - has_x %d\n", data->has_x);
        ei_x_free(&data->x);
    }
    if(data->errorlen >= 0) {
        LOG("free qwork apply - errorlen %d\n", data->errorlen);
        genq_free(data->error);
    }
    LOG("free qwork apply - struct%s\n", "");
    genq_free(data);
}

void free_qwork_hkill(QWorkHKill* data) {
    if(!data) return;
    if(data->errorlen >= 0) {
        LOG("free qwork hkill - errorlen %d\n", data->errorlen);
        genq_free(data->error);
    }
    genq_free(data);
}

void free_qwork_decodebinary(QWorkDecodeBinary* data) {
    if(!data) return;
    if(data->binary) {
        r0(data->binary);
    }
    if(data->errorlen >= 0) {
        genq_free(data->error);
    }
    if(data->has_x) {
        ei_x_free(&data->x);
    }
    genq_free(data);
}

void free_qwork_dbop(QWorkDbOp *data) {
    if(!data) return;
    if(data->has_x) {
        LOG("free qwork dbop - has_x %d\n", data->has_x);
        ei_x_free(&data->x);
    }
    if(data->errorlen >= 0) {
        LOG("free qwork dbop - errorlen %d\n", data->errorlen);
        genq_free(data->error);
    }
    LOG("free qwork apply - struct%s\n", "");
    genq_free(data);
}

