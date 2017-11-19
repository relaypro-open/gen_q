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
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

int ei_x_encode_apply_result(QWorkApply* data, K r, QOpts* opts);
int ei_x_encode_dbop_result(QWorkDbOp* data, K r, QOpts* opts);
int ei_x_encode_decodebinary_result(QWorkDecodeBinary* data, K r, QOpts* opts);
void configure_socket(QWorkHOpen* data);

K db_column(K kdata, const char* column_name);
int db_column_pos(K kdata, const char* column_name);

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

int str_ends_with(const char *str, const char *suffix) {
    if (!str || !suffix)
        return 0;
    size_t lenstr = strlen(str);
    size_t lensuffix = strlen(suffix);
    if (lensuffix >  lenstr)
        return 0;
    return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

void q_dbopen(QWorkDbOp* data, QOpts* opts){
    LOG("dbopen %d\n", 0);

    // Decode the input table which contains all the file paths necessary
    // for reading this db partition.
    K table;
    int ei_res = ei_decode_k(data->buff, &data->types_index,
            &data->values_index, &table, opts);
    if(ei_res < 0) {
        HANDLE_ERROR("ei_decode_k", 11);
        LOG("dbopen ei error - %s\n", "ei_decode_k");
        return;
    }

    // The following block examines all files int he input table:
    //   1. Opens a file handle to each
    //   2. For the sym file, loads it into memory in its entirety
    //   3. Discovers the k-type of each file
    //   4. Initializes the file pos for each file so that reading data
    //      can follow.
    FILE *fptr;
    K filename_column = db_column(table, "filename");
    K column_data_column = db_column(table, "column_data");
    K file_handle_column = db_column(table, "file_handle");
    K data_handle_column = db_column(table, "data_handle");
    K symdata = 0;
    for(int i=0; i<filename_column->n; ++i) {
        LOG("db open %s\n", kS(filename_column)[i]);
        LOG("db open %s\n", kS(column_data_column)[i]);
        fptr = fopen(kS(filename_column)[i], "r");
        kJ(file_handle_column)[i] = (unsigned long long)fptr;

        const char* data_col_file = kS(column_data_column)[i];
        if(str_ends_with(data_col_file, "/sym")) {
            if(symdata == 0) {
                struct stat s;
                int fd = open(data_col_file, O_RDONLY);
                LOG("dbopen sym fd %d\n", fd);
                int status = fstat(fd, &s);
                LOG("dbopen sym status %d\n", status);
                int size = s.st_size;
                LOG("dbopen sym size %d\n", size);
                K sym_bytes = ktn(KG, size);

                void* mmap_data = mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
                if(mmap_data == MAP_FAILED) {
                    LOG("dbopen sym mmap failed %d\n", -1);
                    HANDLE_ERROR("sym", 3);
                }
                LOG("dbopen sym copying %d bytes\n", size);
                memcpy(kG(sym_bytes), mmap_data, size);

                LOG("dbopen sym unmapping %d\n", size);
                munmap(mmap_data, size);

                LOG("dbopen sym closing %d\n", fd);
                close(fd);

                LOG("dbopen sym deserializing bytes %d\n", size);
                int ok = okx(sym_bytes);
                LOG("dbopen sym okx %d\n", ok);
                if(ok) {
                    // This segfaults because the on-disk representation
                    // has a different header than the IPC representation that
                    // d9() expects. I haven't found any documentation about
                    // the on-disk header, but I think we can reverse
                    // engineer it well enough to read simple tables
                    //
                    // I wonder what happens if we try to call the "dot" function
                    // with either `get or `:.
                    symdata = d9(sym_bytes);
                    HANDLE_K_ERRNO(r0(sym_bytes));
                } else {
                    HANDLE_ERROR("okx", 3);
                }
                LOG("dbopen sym done deserializing bytes %d\n", size);

                LOG("db open sym file: "FMT_KN"\n", sym_bytes->n);
                r0(sym_bytes);
            }
            // Magic number (-11) to signify that this sym data is held in
            // memory in the dbstate
            kJ(data_handle_column)[i] = -KS;
        } else {
            fptr = fopen(data_col_file, "r");
            kJ(data_handle_column)[i] = (unsigned long long)fptr;
        }
    }

    // The following section organizes all the data into a dbstate object,
    // which is a dict:
    //      table: The input table, file handles, file positions, etc
    //      sym: The sym file in memory
    // The reference to this object is stored in the QWorkDbOp and given
    // back to the Erlang caller as the state.
    K r_key = ktn(KS, 2);
    kS(r_key)[0]=ss("table");
    kS(r_key)[1]=ss("sym");
    K r_val = knk(2, table, symdata);
    K dbstate = xD(r_key, r_val);
    data->handle = (unsigned long long)dbstate;

    // Store a generic "ok" to represent success.
    K r = ks(ss("ok"));
    int result = ei_x_encode_dbop_result(data, r, opts);
    r0(r);
    if(result < 0) {
        HANDLE_ERROR("eix", 3);
        return;
    }
}
void q_dbnext(QWorkDbOp* data, QOpts* opts){
    HANDLE_ERROR("access", 6);
}
void q_dbclose(QWorkDbOp* data, QOpts* opts){
    // Close all file handles
    //
    K dbstate = (K)data->handle;
    kx_guarded_decr_ref(dbstate);

    K r = ks(ss("ok"));
    int result = ei_x_encode_dbop_result(data, r, opts);
    r0(r);
    if(result < 0) {
        HANDLE_ERROR("eix", 3);
        return;
    }
}

K db_column(K kdata, const char* column_name) {
    int pos = db_column_pos(kdata, column_name);
    if(pos < 0) {
        return 0;
    }
    K values = kK(kdata->k)[1];
    return kK(values)[pos];
}

int db_column_pos(K kdata, const char* column_name) {
    K keys = kK(kdata->k)[0];
    int i=0;
    for(i=0; i<keys->n; ++i) {
        const char* key_name = kS(keys)[i];
        if(0==strcmp(column_name, key_name)) {
            return i;
        }
    }
    return -1;
}

int ei_x_encode_dbop_result(QWorkDbOp* data, K r, QOpts* opts) {
    EI(ei_x_new(&data->x));
    data->has_x = 1;
    EI(ei_x_encode_k(&data->x, r, opts));
    return 0;
}
