#include <stdio.h>
#include "erl_driver.h"
#include "ei.h"
#include "gen_q_work.h"
#include "gen_q_log.h"
#include "k.h"
#include "ei_util.h"

typedef struct {
    ErlDrvPort port;
    QOpts opts;
} GenQData;

static ErlDrvData gen_q_drv_start(ErlDrvPort port, char* buff) {
    open_log();
    GenQData* d = (GenQData*)driver_alloc(sizeof(GenQData));
    d->port = port;
    khp("",-1);
    LOG("port started %d\n", 0);
    return (ErlDrvData)d;
}

static void gen_q_drv_stop(ErlDrvData handle) {
    driver_free((char*)handle);
    LOG("port stopped %d\n", 0);
    close_log();
}

static void gen_q_drv_output(ErlDrvData handle, char *buff,
        ErlDrvSizeT bufflen) {
    GenQData *d = (GenQData*)handle;

    QWork* work = genq_malloc_work(buff, bufflen);
    if(work->op == FUNC_OPTS) {
        copy_qopts((QOpts*)work->data, &d->opts);

        ei_x_buff ok;
        ei_x_encode_ok(&ok);
        driver_output(d->port, ok.buff, ok.index);
        ei_x_free(&ok);
        genq_free_work(work);
        return;
    }
    work->opts = &d->opts;
    driver_async(d->port, NULL,
            genq_work,
            work,
            genq_free_work);
}

static void ready_async(ErlDrvData handle, ErlDrvThreadData w) {
    GenQData* d = (GenQData*)handle;
    QWork* work = (QWork*)w;

    ei_x_buff result;
    ei_x_new(&result);

    genq_work_result(work, &result);
    genq_free_work(work);

    driver_output(d->port, result.buff, result.index);
    ei_x_free(&result);
}

ErlDrvEntry gen_q_drv_entry = {
    NULL,           /* F_PTR init, called when driver is loaded */
    gen_q_drv_start,      /* L_PTR start, called when port is opened */
    gen_q_drv_stop,       /* F_PTR stop, called when port is closed */
    gen_q_drv_output,     /* F_PTR output, called when erlang has sent */
    NULL,           /* F_PTR ready_input, called when input descriptor ready */
    NULL,           /* F_PTR ready_output, called when output descriptor ready */
    "gen_q_drv",      /* char *driver_name, the argument to open_port */
    NULL,           /* F_PTR finish, called when unloaded */
    NULL,                       /* void *handle, Reserved by VM */
    NULL,           /* F_PTR control, port_command callback */
    NULL,           /* F_PTR timeout, reserved */
    NULL,           /* F_PTR outputv, reserved */
    ready_async,                /* F_PTR ready_async, only for async drivers */
    NULL,                       /* F_PTR flush, called when port is about
                                   to be closed, but there is data in driver
                                   queue */
    NULL,                       /* F_PTR call, much like control, sync call
                                   to driver */
    NULL,                       /* F_PTR event, called when an event selected
                                   by driver_event() occurs. */
    ERL_DRV_EXTENDED_MARKER,    /* int extended marker, Should always be
                                   set to indicate driver versioning */
    ERL_DRV_EXTENDED_MAJOR_VERSION, /* int major_version, should always be
                                       set to this value */
    ERL_DRV_EXTENDED_MINOR_VERSION, /* int minor_version, should always be
                                       set to this value */
    0,                          /* int driver_flags, see documentation */
    NULL,                       /* void *handle2, reserved for VM use */
    NULL,                       /* F_PTR process_exit, called when a
                                   monitored process dies */
    NULL                        /* F_PTR stop_select, called to close an
                                   event object */
};

DRIVER_INIT(gen_q_drv) {
    return &gen_q_drv_entry;
}
