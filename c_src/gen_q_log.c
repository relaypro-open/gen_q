#include "gen_q_log.h"
#include <stdio.h>

#if LOGGING_ENABLED
/* log file */
static FILE *fp = NULL;
#endif

int open_log() {
#if LOGGING_ENABLED
    fp = fopen("/Users/jstimpson/dev/erlang/gen_q/.gen_q/log/gen_q_drv.log", "w");

    if( fp ) {
        return 0;
    } else {
        return 1;
    }
#else
    return 0;
#endif
}

void close_log() {
#if LOGGING_ENABLED
    fclose(fp);
#endif
}

FILE* log_file() {
#if LOGGING_ENABLED
    return fp;
#else
    return 0;
#endif
}
