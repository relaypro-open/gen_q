#include "q.h"
#include "gen_q_log.h"
#include "k.h"
#include <string.h>
#include "ei_util.h"

#define HANDLE_K_ERRNO                                                \
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
    }

void q_hopen(QWorkHOpen* data) {

    LOG("khpun %s %ld %s %ld\n", data->host, data->port, data->userpass, data->timeout);

    errno = 0;
    data->handle = khpun(data->host, data->port, data->userpass, data->timeout);
    HANDLE_K_ERRNO;

    LOG("khpun result %d\n", data->handle);
}

void q_hclose(QWorkHClose* data) {
    LOG("kclose %ld\n", data->handle);

    errno = 0;
    kclose(data->handle);
    HANDLE_K_ERRNO;
}

void q_apply(QWorkApply* data) {
}
