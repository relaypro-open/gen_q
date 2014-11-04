#include "gen_q_work.h"
#include <unistd.h>
#include <stdio.h>
#include "ei.h"

void genq_work(void *data) {
    QWork *work = (QWork*)data;
    sleep(work->timeout / 1000);
}

void* genq_malloc_work(char *buff, ErlDrvSizeT bufflen) {
    QWork *work = malloc(sizeof(QWork));

    int index = 0;
    ei_decode_version(buff, &index, &work->version);
    ei_decode_long(buff, &index, &work->timeout);

    return work;
}

void genq_free_work(void *work) {
    free(work);
}
