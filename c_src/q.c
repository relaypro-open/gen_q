#include "q.h"
#include "gen_q.h"
#include "gen_q_log.h"
#include "k.h"
#include <string.h>
#include "ei_util.h"
#include "e2q.h"
#include "q2e.h"
#include "gen_q.h"
#include <netinet/tcp.h>

int ei_x_encode_apply_result(QWorkApply* data, K r, QOpts* opts);
int ei_x_encode_decodebinary_result(QWorkDecodeBinary* data, K r, QOpts* opts);
void configure_socket(QWorkHOpen* data);

#define HANDLE_K_ERRNO(cleanup)                                       \
    LOG("checking errno %d\n", 0);                                    \
    if(errno) {                                                       \
        char ebuf[256];                                               \
        if(0!=strerror_r(errno, ebuf, 256)) {                         \
            LOG("k unexpected error: errno=%d - %s\n", errno, ebuf);  \
        } else {                                                      \
            int elen = strlen(ebuf);                                  \
            data->error = genq_alloc((sizeof(char))*(elen+1));            \
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
        data->error = genq_alloc((sizeof(char))*(data->errorlen+1)); \
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
    data->handle = khpun((S)data->host, data->port, (S)data->userpass, data->timeout);
    HANDLE_K_ERRNO(/* No cleanup */);

    LOG("khpun result %d\n", data->handle);

    if(data->handle == 0) {
        HANDLE_ERROR("access", 6);
        return;
    }

    configure_socket(data);
}

void configure_socket(QWorkHOpen* data) {
    int ensure_nodelay = 1;
    if(ensure_nodelay) {

        int nodelay_on = 0;
        socklen_t opt_len = 0;
        int sock_result = getsockopt(data->handle, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay_on, &opt_len);
        HANDLE_K_ERRNO( kclose(data->handle) );

        LOG("TCP_NODELAY getsockopt got %d, on==%d\n", sock_result, nodelay_on);

        if(sock_result < 0) {
            HANDLE_ERROR("nodelay-r", 9);
            kclose(data->handle);
            return;
        }

        if(!nodelay_on) {
            nodelay_on = 1;
            sock_result = setsockopt(data->handle, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay_on, sizeof(int));
            HANDLE_K_ERRNO( kclose(data->handle) );

            if(sock_result < 0) {
                HANDLE_ERROR("nodelay-w", 9);
                kclose(data->handle);
                return;
            }

            int r = getsockopt(data->handle, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay_on, &opt_len);

            LOG("TCP_NODELAY setsockopt got %d, on==%d\n", sock_result, nodelay_on);
            sock_result = r;

            if(sock_result < 0) {
                HANDLE_ERROR("nodelay-c", 9);
                kclose(data->handle);
                return;
            }
        }
    }
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

    // -------------
    // handle kdata
    if(kdata) {
        LOG("kapply calling %s with kdata->n size "FMT_KN"\n", data->func, kdata->n);
    } else {
        LOG("kapply calling %s with (::)\n", data->func);
    }
    errno = 0;
    K r = k(data->handle, (S)data->func, kdata, (K)0);
    if(r) {
        LOG("kapply received result with type %d\n", r->t);
    }
    HANDLE_K_ERRNO(kx_guarded_decr_ref(r));

    if(data->handle < 0) {
        LOG("kapply asynchonous call, returning ok%s\n", "");
        r0(r);
        ei_x_new(&data->x);
        data->has_x = 1;
        ei_x_encode_atom(&data->x, "ok");
        return;
    }

    if(!r) {
        HANDLE_ERROR("null", 4);
        return;
    }

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
    EI(ei_x_encode_k(&data->x, r, opts));
    return 0;
}

void q_hkill(QWorkHKill* data) {
    LOG("killing %ld\n", data->handle);
 
    errno = 0;
    k((I)data->handle, (const S)"\\\\", (K)0);
    HANDLE_K_ERRNO(/* No cleanup */);
}

void q_decodebinary(QWorkDecodeBinary* data, QOpts* opts) {
    LOG("decodebinary "FMT_KN"\n", data->binary->n);
    errno = 0;
    K r = d9(data->binary);
    HANDLE_K_ERRNO(/* No cleanup */);
    if(!r) {
        HANDLE_ERROR("bin", 3);
        return;
    }

    int result = ei_x_encode_decodebinary_result(data, r, opts);
    r0(r);
    if(result < 0) {
        HANDLE_ERROR("eix", 3);
        return;
    }
}

int ei_x_encode_decodebinary_result(QWorkDecodeBinary* data, K r, QOpts* opts) {
    EI(ei_x_new(&data->x));
    data->has_x = 1;
    EI(ei_x_encode_k(&data->x, r, opts));
    return 0;
}
