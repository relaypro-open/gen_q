#include "q.h"
#include "gen_q_log.h"
#include "k.h"
#include <string.h>
#include "ei_util.h"
#include "e2q.h"
#include "q2e.h"

int ei_x_encode_apply_result(QWorkApply* data, K r, QOpts* opts);

#define HANDLE_K_ERRNO(cleanup)                                       \
    LOG("checking errno %d\n", 0);                                    \
    if(errno) {                                                       \
        char ebuf[256];                                               \
        if(0!=strerror_r(errno, ebuf, 256)) {                         \
            LOG("k unexpected error: errno=%d - %s\n", errno, ebuf);  \
        } else {                                                      \
            int elen = strlen(ebuf);                                  \
            data->error = malloc((sizeof(char))*(elen+1));            \
            memcpy(data->error, ebuf, elen);                          \
            data->errorlen = elen;                                    \
            (data->error)[elen] = '\0';                               \
        }                                                             \
        cleanup;                                                      \
        return;                                                       \
    }

#define HANDLE_ERROR(errstr, errlen) \
    do { \
        data->errorlen = errlen; \
        data->error = malloc((sizeof(char))*(data->errorlen+1)); \
        memcpy(data->error, errstr, data->errorlen); \
        data->error[data->errorlen] = '\0'; \
    } while(0)

/* Decrements the ref count of object r if r is non-NULL */
void kx_guarded_decr_ref(K r) {
    if(r)
        r0(r);
}

void q_hopen(QWorkHOpen* data) {

    LOG("khpun %s %ld %s %ld\n", data->host, data->port, data->userpass, data->timeout);

    errno = 0;
    data->handle = khpun(data->host, data->port, data->userpass, data->timeout);
    HANDLE_K_ERRNO(/* No cleanup */);

    LOG("khpun result %d\n", data->handle);
}

void q_hclose(QWorkHClose* data) {
    LOG("kclose %ld\n", data->handle);

    errno = 0;
    kclose(data->handle);
    HANDLE_K_ERRNO(/* No cleanup */);
}

void q_apply(QWorkApply* data, QOpts* opts) {
    LOG("kapply %d\n", 0);

    K kdata;
    int ei_res = ei_decode_k(data->buff, &data->types_index, &data->values_index, &kdata, opts);
    if(ei_res < 0) {
        HANDLE_ERROR("ei_decode_k", 11);
        LOG("kapply ei error - %s\n", "ei_decode_k");
        return;
    }
    /*
    if(!kdata) {
        HANDLE_ERROR("e2q", 3);
        LOG("kapply null kdata - %s\n", "e2q");
        return;
    }*/

    // -------------
    // handle kdata
    if(kdata) {
        LOG("kapply calling %s with kdata->n size %lld\n", data->func, kdata->n);
    } else {
        LOG("kapply calling %s with (::)\n", data->func);
    }
    errno = 0;
    K r = k(data->handle, data->func, kdata, (K)0);
    LOG("kapply received result with type %d\n", r->t);

    HANDLE_K_ERRNO(/* No cleanup */);

    if(r->t == -128) {
        // q error
        LOG("kapply received q error %s\n", r->s);
        HANDLE_ERROR(r->s, strlen(r->s));

        r0(r);
        return;
    }

    int encode_result = ei_x_encode_apply_result(data, r, opts);

    r0(r);

    if(encode_result < 0) {
        HANDLE_ERROR("eix", 3);
        LOG("kapply encode error - %s\n", "eix");
        return;
    }
    LOG("kapply encode success %d\n", 0);
}

int ei_x_encode_apply_result(QWorkApply* data, K r, QOpts* opts) {
    EI(ei_x_new(&data->x));
    data->has_x = 1;
    //EI(ei_x_encode_atom(&data->x, "ok"));
    EI(ei_x_encode_k(&data->x, r, opts));
    return 0;
}

 void q_hkill(QWorkHKill* data) {
    LOG("killing %ld\n", data->handle);

    errno = 0;
    k((I)data->handle, (const S)"\\\\", (K)0);
    HANDLE_K_ERRNO(/* No cleanup */);
 }
