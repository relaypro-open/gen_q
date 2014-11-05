#include "q.h"
#include "gen_q_log.h"
#include "k.h"
#include <string.h>
#include "ei_util.h"

/* This macro expands to a block of code that does error checking on an errno
 *  * that kx may have set. The caller is to provide an expression in the argument
 *   * to perform necessary cleanup operations. This expression is inserted into
 *    * code block before each return statement. */
#define HANDLE_K_ERRNO(cleanupFuncExpression)                            \
    if(errno) {                                                          \
        char ebuf[256];                                                  \
        if(0!=strerror_r(errno, ebuf, 256)) {                            \
            cleanupFuncExpression;                                       \
            LOG("kx unexpected error: errno=%d - %s\n", errno, ebuf);    \
            return ei_x_encode_error_tuple_atom(buff, "c_string_error"); \
        }                                                                \
        cleanupFuncExpression;                                           \
        return ei_x_encode_error_tuple_string(buff, ebuf);               \
    }

int q_hopen(char *host, int port, char *unpw, int timeout, ei_x_buff *buff) {
    errno = 0;
    int h = khpun(host, port, unpw, timeout);

    HANDLE_K_ERRNO(/* No cleanup */);

    if(h < 0) {
        return ei_x_encode_error_tuple_atom(buff, "conn_failed");
    }

    EI_X_ENC(ei_x_encode_ok_tuple_header(buff));
    EI_X_ENC(ei_x_encode_long(buff, h));
    return 0;
}

int q_hclose(int handle, ei_x_buff *buff) {
    errno = 0;
    kclose((I)handle);
    LOG("closed connection with handle %d\n", handle);
    HANDLE_K_ERRNO(/* No cleanup */);
    return OK;
}
