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

#define Q_FILE_MAGIC_4BYTELEN 255 // byte stream is q file with 4-byte vector lengths
#define Q_FILE_MAGIC_8BYTELEN 254 // byte stream is q file iwth 8-byte vector lengths
#define Q_FILE_LITTLE_ENDIAN 0x01
#define Q_FILE_SPLAYED_TABLE 0x20
#define Q_DISK_REGULAR 0
#define Q_DISK_SPLAYED 1
#define Q_DISK_SYMENUM 2
#define Q_DISK_STRDATA 3
#define Q_DISK_ERROR -1

int ei_x_encode_apply_result(QWorkApply* data, K r, QOpts* opts);
int ei_x_encode_dbop_result(QWorkDbOp* data, K r, QOpts* opts);
int ei_x_encode_decodebinary_result(QWorkDecodeBinary* data, K r, QOpts* opts);
int ei_x_q_dbnext(QWorkDbOp* data, long num_records, QOpts* opts);
void configure_socket(QWorkHOpen* data);

K table_column(K kdata, const char* column_name);
int table_column_pos(K kdata, const char* column_name);
K dict_entry(K kdata, const char* name);
int dict_entry_pos(K kdata, const char* name);
K db_read_sym_file(const char* sym_file);

int q_get_disk_file_format(const unsigned char* filebytes, int size);
int q_get_fptr_disk_file_format(FILE *fptr);
int q_get_fptr_ktype(FILE *fptr);
int q_ipc_create_sym(K* kbytes, const unsigned char* filebytes, int size);
int q_get_le(int ktype, const unsigned char* x);
void q_set_le(int ktype, G* x, int y);

void fprintf_i(FILE *fptr, int i);
void fprintf_j(FILE *fptr, long long j);
void fprintf_f(FILE *fptr, double d);
void fprintf_z(FILE *fptr, double d);
void fprintf_p(FILE *fptr, long long j);

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

K db_read_sym_file(const char* sym_file) {
    struct stat s;
    int fd = open(sym_file, O_RDONLY);
    LOG("dbopen sym fd %d\n", fd);
    fstat(fd, &s);
    int size = s.st_size;
    LOG("dbopen sym size %d\n", size);

    void* mmap_data = mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if(mmap_data == MAP_FAILED) {
        LOG("dbopen sym mmap failed %d\n", -1);
        return 0;
    }

    K sym_bytes = 0;
    LOG("dbopen sym copying %d bytes\n", size);
    if(0 != q_ipc_create_sym(&sym_bytes, mmap_data, size)) {
        kx_guarded_decr_ref(sym_bytes);
        return 0;
    }

    LOG("dbopen sym unmapping %d\n", size);
    munmap(mmap_data, size);

    LOG("dbopen sym closing %d\n", fd);
    close(fd);

    LOG("dbopen sym deserializing bytes %d\n", size);
    int ok = okx(sym_bytes);
    LOG("dbopen sym okx %d\n", ok);
    if(ok) {
        errno = 0;
        K symdata = d9(sym_bytes);
        r0(sym_bytes);
        return symdata;
    }
    kx_guarded_decr_ref(sym_bytes);
    return 0;
}

void q_dbopen(QWorkDbOp* data, QOpts* opts){
    LOG("dbopen %d\n", 0);

    // Decode the input table which contains all the file paths necessary
    // for reading this db partition.
    K input;
    int ei_res = ei_decode_k(data->buff, &data->types_index,
            &data->values_index, &input, opts);
    if(ei_res < 0) {
        HANDLE_ERROR("ei_decode_k", 11);
        LOG("dbopen ei error - %s\n", "ei_decode_k");
        return;
    }

    K outputfile = dict_entry(input, "outputfile");
    FILE *outputfile_h = 0;
    K outputfile_append_k = dict_entry(input, "outputfile_append");
    int outputfile_append = 0 == strcmp(outputfile_append_k->s, "true");
    if(0!=strcmp(outputfile->s, "undefined")) {
        if (outputfile_append) 
            outputfile_h = fopen(outputfile->s, "a+");
        else
            outputfile_h = fopen(outputfile->s, "w+");
    }

    K csv_header_k = dict_entry(input, "csv_header");
    int csv_header = 0 == strcmp(csv_header_k->s, "true");

    K return_data = dict_entry(input, "return_data");

    // The following block examines all files in the input table:
    //   1. Opens a file handle to each
    //   2. For the sym file, loads it into memory in its entirety
    //   3. Discovers the k-type of each file
    //   4. Initializes the file pos for each file so that reading data
    //      can follow.
    FILE *fptr;
    K filename_column = dict_entry(input, "filename");
    K column_data_column = dict_entry(input, "column_data");
    K file_handle_column = dict_entry(input, "file_handle");
    K data_handle_column = dict_entry(input, "data_handle");
    K column_type_column = dict_entry(input, "column_type");
    K file_pos_column = dict_entry(input, "file_pos");
    K column_name_column = dict_entry(input, "column_name");
    K symdata = 0;
    int ktype = 0;
    for(int i=0; i<filename_column->n; ++i) {
        LOG("dbopen reading %s\n", kS(filename_column)[i]);
        LOG("dbopen data %s\n", kS(column_data_column)[i]);

        if (outputfile_h != 0 && csv_header) {
            fwrite(kS(column_name_column)[i], 1, strlen(kS(column_name_column)[i]), outputfile_h);
            if(i+1 != filename_column->n) {
                fwrite(",", 1, 1, outputfile_h);
            }
        }

        // Open file
        fptr = fopen(kS(filename_column)[i], "r");
        kJ(file_handle_column)[i] = (unsigned long long)fptr;
        ktype = q_get_fptr_ktype(fptr);
        kJ(column_type_column)[i] = (long)ktype;
        LOG("db open ktype %d\n", ktype);

        kJ(file_pos_column)[i] = 0;

        // Open data file
        const char* data_col_file = kS(column_data_column)[i];
        if(str_ends_with(data_col_file, "/sym")) {
            if(symdata == 0) {
                symdata = db_read_sym_file(data_col_file);
                if(symdata == 0) {
                    HANDLE_ERROR("sym", 3);
                    return;
                }
                for(int j=0; j<50; ++j) {
                    LOG("db open sym %s\n", kS(symdata)[j]);
                }
            }
            // Magic number (-11) to signify that this sym data is held in
            // memory in the dbstate
            kJ(data_handle_column)[i] = -KS;
        } else {
            fptr = fopen(data_col_file, "r");
            kJ(data_handle_column)[i] = (unsigned long long)fptr;
        }
    }

    if (outputfile_h != 0 && csv_header) {
        fwrite("\n", 1, 1, outputfile_h);
    }

    if(symdata == 0) {
        HANDLE_ERROR("sym", 3);
        return;
    }

    // The following section organizes all the data into a dbstate object,
    // which is a dict:
    //      sym: The sym file in memory
    // The reference to this object is stored in the QWorkDbOp and given
    // back to the Erlang caller as the state.
    K r_key = ktn(KS, 10);
    kS(r_key)[0]=ss("outputfile");
    kS(r_key)[1]=ss("return_data");
    kS(r_key)[2]=ss("filename");
    kS(r_key)[3]=ss("column_data");
    kS(r_key)[4]=ss("file_handle");
    kS(r_key)[5]=ss("data_handle");
    kS(r_key)[6]=ss("column_type");
    kS(r_key)[7]=ss("file_pos");
    kS(r_key)[8]=ss("column_name");
    kS(r_key)[9]=ss("sym");
    K r_val = knk(10, 
            kj((unsigned long long)outputfile_h),
            return_data,
            filename_column,
            column_data_column,
            file_handle_column,
            data_handle_column,
            column_type_column,
            file_pos_column,
            column_name_column,
            symdata);
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
    K num_records;
    int ei_res = ei_decode_k(data->buff, &data->types_index,
            &data->values_index, &num_records, opts);
    if(ei_res < 0) {
        HANDLE_ERROR("ei_decode_k", 11);
        LOG("dbnext ei error - %s\n", "ei_decode_k");
        return;
    }

    if(num_records->t != -KJ) {
        HANDLE_ERROR("long", 4);
        return;
    }

    LOG("dbnext getting %lld records\n", num_records->j);

    int result = ei_x_q_dbnext(data, num_records->j, opts);
    if(result < 0) {
        HANDLE_ERROR("eix", 3);
        return;
    }
}

int ei_x_q_dbnext(QWorkDbOp* data, long num_records, QOpts* opts) {
    K dbstate = (K)data->n;
    if (dbstate == 0) {
        LOG("dbnext ERROR state %d\n", 0);
        HANDLE_ERROR("nostate", 7);
        return -1;
    }
    K sym = dict_entry(dbstate, "sym");
    if(sym == 0) {
        LOG("dbnext ERROR sym %d\n", 0);
        HANDLE_ERROR("sym", 3);
        return -1;
    }
    K outputfile_k = dict_entry(dbstate, "outputfile");
    FILE *outputfile_h = (FILE*)outputfile_k->j;
    K return_data_k = dict_entry(dbstate, "return_data");
    LOG("dbnext return_data_k is %s\n", return_data_k->s);
    int return_data = 0==strcmp(return_data_k->s, "true");
    LOG("dbnext return_data is %d\n", return_data);

    K filename_column = dict_entry(dbstate, "filename");
    K file_handle_column = dict_entry(dbstate, "file_handle");
    K data_handle_column = dict_entry(dbstate, "data_handle");
    K column_type_column = dict_entry(dbstate, "column_type");
    K file_pos_column = dict_entry(dbstate, "file_pos");
    K column_name_column = dict_entry(dbstate, "column_name");
    int ktype = 0;
    FILE *fptr;
    long long long_ = 0;
    int int_ = 0;
    double double_ = 0;
    float float_ = 0;
    long pos = 0;
    char char_ = 0;
    short short_ = 0;
    char buffer[1024] = {0};
    int ok = 0;

    int private_data_column_pos = -1;

    EI(ei_x_new(&data->x));
    data->has_x = 1;

    int i=0;
    for(i=0; i < num_records; ++i) {
        for(int j=0; j < column_name_column->n; ++j) {
            LOG("dbnext reading %s\n", kS(filename_column)[j]);
            LOG("dbnext data %s\n", kS(column_name_column)[j]);
            ktype = kJ(column_type_column)[j];
            fptr = (FILE*)kJ(file_handle_column)[j];

            // HACK
            if(private_data_column_pos < 0 &&
                    str_ends_with(kS(column_name_column)[j], "private_data_expiry_0")) {
                private_data_column_pos = j;
            }

            switch (ktype) {
                case KT: // time
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KV: // second
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KU: // minute
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KN: // timespan
                    ok = 1 == fread((void*)&long_, 8, 1, fptr);
                    break;
                case KZ: // datetime
                    ok = 1 == fread((void*)&double_, 8, 1, fptr);
                    break;
                case KD: // date
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KM: // month
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KI: // int
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case KP: // timestamp
                    ok = 1 == fread((void*)&long_, 8, 1, fptr);
                    break;
                case KJ: // long
                    ok = 1 == fread((void*)&long_, 8, 1, fptr);
                    break;
                case KF: // float
                    ok = 1 == fread((void*)&double_, 8, 1, fptr);
                    break;
                case KC: // char
                    ok = 1 == fread((void*)&char_, 1, 1, fptr);
                    break;
                case KE: // real
                    ok = 1 == fread((void*)&float_, 4, 1, fptr);
                    break;
                case KH: // short
                    ok = 1 == fread((void*)&short_, 1, 1, fptr);
                    break;
                case KG: // byte
                    ok = 1 == fread((void*)&char_, 1, 1, fptr);
                    break;
                case KB: // boolean
                    ok = 1 == fread((void*)&char_, 1, 1, fptr);
                    break;
                case KS: // symbol
                    // This typically won't be used since syms in a splayed
                    // table should have an enumeration file (sym)
                    memset(buffer, 0, 1024);
                    ok = 1;
                    for(int_=0; int_<1024; ++int_) {
                        ok |= 1 == fread(buffer+int_, 1, 1, fptr);
                        if (buffer[int_] == 0) {
                            break;
                        }
                    }
                    break;
                case 0: // enum'd symbol
                    ok = 1 == fread((void*)&int_, 4, 1, fptr);
                    break;
                case 87: // special string type
                    pos = kJ(file_pos_column)[j];
                    LOG("dbnext string pos %ld\n", pos);
                    ok = 1 == fread((void*)&long_, 8, 1, fptr);

                    break;
                default:
                    LOG("dbnext unhandled type %d\n", ktype);
                    break;
            }

            // WRITE DATA TO CSV
            if (outputfile_h != 0) {
                if (j != private_data_column_pos) {
                    switch (ktype) {
                        case KT: // time
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KV: // second
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KU: // minute
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KN: // timespan
                            //EI(ei_x_encode_kj_val(&data->x, long_));
                            fprintf_j(outputfile_h, long_);
                            break;
                        case KZ: // datetime
                            //EI(ei_x_encode_datetime_as_now(&data->x, double_));
                            fprintf_z(outputfile_h, double_);
                            break;
                        case KD: // date
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KM: // month
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KI: // int
                            //EI(ei_x_encode_ki_val(&data->x, int_));
                            fprintf_i(outputfile_h, int_);
                            break;
                        case KP: // timestamp
                            //EI(ei_x_encode_timestamp_as_now(&data->x, long_));
                            fprintf_p(outputfile_h, long_);
                            break;
                        case KJ: // long
                            //EI(ei_x_encode_kj_val(&data->x, long_));
                            fprintf_j(outputfile_h, long_);
                            break;
                        case KF: // float
                            //EI(ei_x_encode_kf_val(&data->x, double_));
                            fprintf_f(outputfile_h, double_);
                            break;
                        case KC: // char
                            //EI(ei_x_encode_char(&data->x, char_));
                            fprintf(outputfile_h, "%c", char_);
                            break;
                        case KE: // real
                            //EI(ei_x_encode_kf_val(&data->x, float_));
                            fprintf_f(outputfile_h, double_);
                            break;
                        case KH: // short
                            //EI(ei_x_encode_ki_val(&data->x, short_));
                            fprintf_i(outputfile_h, (int)short_);
                            break;
                        case KG: // byte
                            //EI(ei_x_encode_char(&data->x, char_));
                            fprintf_i(outputfile_h, (int)char_);
                            break;
                        case KB: // boolean
                            if(char_) {
                                //    EI(ei_x_encode_atom(&data->x, "true"));
                                fwrite("true", 1, 4, outputfile_h);
                            } else {
                                //    EI(ei_x_encode_atom(&data->x, "false"));
                                fwrite("false", 1, 5, outputfile_h);
                            }
                            break;
                        case KS: // symbol
                            // This typically won't be used since syms in a splayed
                            // table should have an enumeration file (sym)
                            if (int_ == 1024) {
                                //    EI(ei_x_encode_atom(&data->x, "null"));
                            } else {
                                //    EI(ei_x_encode_binary(&data->x, buffer, int_));
                                fprintf(outputfile_h, "%s", buffer);
                            }
                            break;
                        case 0: // enum'd symbol
                            if(int_ > 0 && int_ < sym->n) {
                                //    const char *s = kS(sym)[int_];
                                //    int len = strlen(s);
                                //    LOG("dbnext symbol index %d %s, len %d\n",
                                //            int_, s, (int)len);
                                //    EI(ei_x_encode_binary(&data->x, s, len));
                                fprintf(outputfile_h, "%s", kS(sym)[int_]);
                            } else {
                                //    // null symbol
                                //    EI(ei_x_encode_atom(&data->x, "null"));
                            }
                            break;
                        case 87: // special string type
                            //LOG("dbnext string val %ld\n", long_);
                            fptr = (FILE*)kJ(data_handle_column)[j];

                            char* string_ = genq_alloc((sizeof(char))*(long_-pos));
                            ok = (long_-pos) == fread(string_, 1, long_-pos, fptr);
                            if(!ok) {
                                //    EI(ei_x_encode_atom(&data->x, "null"));
                                ok = 1;
                            } else {
                                //    EI(ei_x_encode_binary(&data->x, string_, long_-pos));
                                fwrite(string_, 1, long_-pos, outputfile_h);
                            }
                            genq_free(string_);

                            kJ(file_pos_column)[j] = long_;

                            break;
                        default:
                            LOG("dbnext unhandled type in csv path %d\n", ktype);
                            break;
                    }
                }
                if(j+1 != column_name_column->n) {
                    fwrite(",", 1, 1, outputfile_h);
                }
            }

            if(!return_data) {
                continue;
            }

            // WRITE DATA TO EI
            if(ok && j == 0) {
                EI(ei_x_encode_list_header(&data->x, 1));
                EI(ei_x_encode_map_header(&data->x, column_name_column->n));
            } else if(!ok && j == 0) {
                break;
            } else if(!ok) {
                // We already started the map, can't back out now!
                EI(ei_x_encode_atom(&data->x, kS(column_name_column)[j]));
                EI(ei_x_encode_atom(&data->x, "null"));
                continue;
            }

            EI(ei_x_encode_atom(&data->x, kS(column_name_column)[j]));

            // HACK
            if(j == private_data_column_pos) {
                EI(ei_x_encode_tuple_header(&data->x, 2));
                EI(ei_x_encode_atom(&data->x, "binary"));
            }

            switch (ktype) {
                case KT: // time
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KV: // second
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KU: // minute
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KN: // timespan
                    EI(ei_x_encode_kj_val(&data->x, long_));
                    break;
                case KZ: // datetime
                    EI(ei_x_encode_datetime_as_now(&data->x, double_));
                    break;
                case KD: // date
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KM: // month
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KI: // int
                    EI(ei_x_encode_ki_val(&data->x, int_));
                    break;
                case KP: // timestamp
                    EI(ei_x_encode_timestamp_as_now(&data->x, long_));
                    break;
                case KJ: // long
                    EI(ei_x_encode_kj_val(&data->x, long_));
                    break;
                case KF: // float
                    EI(ei_x_encode_kf_val(&data->x, double_));
                    break;
                case KC: // char
                    EI(ei_x_encode_char(&data->x, char_));
                    break;
                case KE: // real
                    EI(ei_x_encode_kf_val(&data->x, float_));
                    break;
                case KH: // short
                    EI(ei_x_encode_ki_val(&data->x, short_));
                    break;
                case KG: // byte
                    EI(ei_x_encode_char(&data->x, char_));
                    break;
                case KB: // boolean
                    if(char_) {
                        EI(ei_x_encode_atom(&data->x, "true"));
                    } else {
                        EI(ei_x_encode_atom(&data->x, "false"));
                    }
                    break;
                case KS: // symbol
                    // This typically won't be used since syms in a splayed
                    // table should have an enumeration file (sym)
                    if (int_ == 1024) {
                        EI(ei_x_encode_atom(&data->x, "null"));
                    } else {
                        EI(ei_x_encode_binary(&data->x, buffer, int_));
                    }
                    break;
                case 0: // enum'd symbol
                    if(int_ > 0 && int_ < sym->n) {
                        const char *s = kS(sym)[int_];
                        int len = strlen(s);
                        LOG("dbnext symbol index %d %s, len %d\n",
                                int_, s, (int)len);
                        EI(ei_x_encode_binary(&data->x, s, len));
                    } else {
                        // null symbol
                        EI(ei_x_encode_atom(&data->x, "null"));
                    }
                    break;
                case 87: // special string type
                    LOG("dbnext string val %lld\n", long_);
                    fptr = (FILE*)kJ(data_handle_column)[j];

                    char* string_ = genq_alloc((sizeof(char))*(long_-pos));
                    ok = (long_-pos) == fread(string_, 1, long_-pos, fptr);
                    if(!ok) {
                        EI(ei_x_encode_atom(&data->x, "null"));
                        ok = 1;
                    } else {
                        EI(ei_x_encode_binary(&data->x, string_, long_-pos));
                    }
                    genq_free(string_);

                    kJ(file_pos_column)[j] = long_;

                    break;
                default:
                    LOG("dbnext unhandled type %d\n", ktype);
                    EI(ei_x_encode_atom(&data->x, "null"));
                    break;
            }
        }
        if (outputfile_h != 0) {
            fwrite("\n", 1, 1, outputfile_h);
        }
        if(!ok) {
            break;
        }
    }

    if(return_data) {
        EI(ei_x_encode_empty_list(&data->x));
    } else {
        EI(ei_x_encode_long(&data->x, i));
    }
    return 0;
}

void fprintf_i(FILE *fptr, int i) {
    if (i == ni ||
            i == wi) {
        return;
    }
    fprintf(fptr, "%d", i);
}

void fprintf_j(FILE *fptr, long long j) {
    if (j == nj ||
            j == wj) {
        return;
    }
    fprintf(fptr, "%lld", j);
}

void fprintf_f(FILE *fptr, double d) {
    if(d == nh || d == wh || d!=d) {
        return;
    }
    fprintf(fptr, "%f", d);
}

void fprintf_z(FILE *fptr, double d) {
    if(d == nf || d == wf || d != d) return;
    long long unix_micros = datetime_to_unix_micros(d);
    fprintf_j(fptr, unix_micros);
}

void fprintf_p(FILE *fptr, long long j) {
    if(j == nj || j == wj) return;
    long long unix_micros = timestamp_to_unix_micros(j);
    fprintf_j(fptr, unix_micros);
}

void q_dbclose(QWorkDbOp* data, QOpts* opts){
    // Close all file handles
    //
    FILE *fptr;
    long long data_f = 0;
    K dbstate = (K)data->n;
    if (dbstate == 0) {
        HANDLE_ERROR("nostate", 7);
        return;
    }
    K outputfile_h = dict_entry(dbstate, "outputfile");
    if(outputfile_h != 0 &&
            outputfile_h->j != 0) {
        LOG("dbclose closing output file %lld\n", outputfile_h->j);
        fclose((FILE*)outputfile_h->j);
    }
            
    K file_handle_column = dict_entry(dbstate, "file_handle");
    K data_handle_column = dict_entry(dbstate, "data_handle");
    for(int i=0; i<file_handle_column->n; ++i) {
        fptr = (FILE*)kJ(file_handle_column)[i];
        if(fptr != 0) {
            LOG("dbclose closing file %lld\n", (unsigned long long)fptr);
            fclose(fptr);
        }
        data_f = kJ(data_handle_column)[i];
        if(data_f > 0) {
            LOG("dbclose closing data file %lld\n", data_f);
            fptr = (FILE*)data_f;
            fclose(fptr);
        }
    }
    kx_guarded_decr_ref(dbstate);

    K r = ks(ss("ok"));
    int result = ei_x_encode_dbop_result(data, r, opts);
    r0(r);
    if(result < 0) {
        HANDLE_ERROR("eix", 3);
        return;
    }
}

K table_column(K kdata, const char* column_name) {
    if(kdata->t != XT) {
        return 0;
    }
    int pos = table_column_pos(kdata, column_name);
    if(pos < 0) {
        return 0;
    }
    K values = kK(kdata->k)[1];
    return kK(values)[pos];
}

int table_column_pos(K kdata, const char* column_name) {
    if(kdata->t != XT) {
        return 0;
    }
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

K dict_entry(K kdata, const char* name) {
    if(kdata->t != XD) {
        return 0;
    }
    LOG("dict pos %d\n", 0);
    int pos = dict_entry_pos(kdata, name);
    if(pos < 0) {
        return 0;
    }
    K values = kK(kdata)[1];
    return kK(values)[pos];
}

int dict_entry_pos(K kdata, const char* name) {
    if(kdata->t != XD) {
        return 0;
    }
    K keys = kK(kdata)[0];
    if(keys == 0) {
        return -1;
    }
    int i=0;
    for(i=0; i<keys->n; ++i) {
        const char* key_name = kS(keys)[i];
        if(0==strcmp(name, key_name)) {
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

int q_get_disk_file_format(const unsigned char* filebytes, int size) {
    // possible a string data file, shouldn't be passed in here
    if (size < 9) {
        return Q_DISK_ERROR;
    }
    if (filebytes[0] == Q_FILE_MAGIC_4BYTELEN) {
        if (filebytes[1] == Q_FILE_LITTLE_ENDIAN) {
            // regular file (e.g. sym file)
            // Type is lower short of byte 2
            // Attr is byte 3
            // Vec len is pos 4-7
            // Data starts at pos 8
            // null-terminated strings follow
            return Q_DISK_REGULAR;
        } else if (filebytes[1] == Q_FILE_SPLAYED_TABLE) {
            // file that is part of a splayed table
            // Type is lower short of byte as pos 8
            // Attr is pos 10
            // Vec len is pos 12-15
            // Data starts at pos 16
            // Data size according to type
            //
            // Note: if type is 0x57, this is a string index file. Notice that
            // the lower short is 0x7, long type, higher short is 0x5, which seems
            // to be a modifier on the type. Each long corresponds to the first
            // position of the next string in the file. In other words
            // (pos - prevpos) is the string length, and prevpos defaults to 0 for
            // the first string
            return Q_DISK_SPLAYED;
        } else if (filebytes[1] == 0) {
            return Q_DISK_ERROR;
        } else {
            if (filebytes[8] == 0) {
                // sym enumeration.
                // For enumeration name: read bytes starting at pos 1 until
                // max pos 7, look for null term, or add in null term.
                //
                // Attr is at pos 10
                // Vec len is pos 12-15, little endian
                // Data starts at pos 16
                // Data is 4-byte ints, index to sym file
                return Q_DISK_SYMENUM;
            }
        }
    } else if (filebytes[0] == Q_FILE_MAGIC_8BYTELEN) {
        // I think 0xfe means vec len is 8 bytes instead of 4
        return Q_DISK_ERROR;
    }

    // assume its a string data file.
    // Strings are not null-terminated, just have to rely on index list
    // in corresponding file. (type 0x57 above)
    return Q_DISK_STRDATA;
}

int q_get_fptr_disk_file_format(FILE *fptr) {
    const int hdr_buf_size = 9;
    unsigned char hdr_buf[hdr_buf_size] = {0};
    int read_bytes = fread(hdr_buf, 1, hdr_buf_size, fptr);
    if (read_bytes < hdr_buf_size){
        return Q_DISK_ERROR;
    }

    return q_get_disk_file_format(hdr_buf, hdr_buf_size);
}

int q_get_fptr_ktype(FILE *fptr) {
    int ftype = q_get_fptr_disk_file_format(fptr);
    char ktype = 0;
    switch (ftype) {
        case Q_DISK_SPLAYED:
            fseek(fptr, 8, SEEK_SET);
            fread((void*)&ktype, 1, 1, fptr);
            fseek(fptr, 16, SEEK_SET);
            return ktype;
        case Q_DISK_SYMENUM:
            fseek(fptr, 16, SEEK_SET);
            return 0;
        case Q_DISK_STRDATA:
            fseek(fptr, 0, SEEK_SET);
            return -1;
    }

    return -1;
}

int q_ipc_create_sym(K* kbytes, const unsigned char* filebytes, int size) {
    // NOTE : much of this function is general purpose, but there is a 
    // type == KS check that makes it specific to sym file for now
    //
    if (size < 8) {
        return -1;
    }
    int ftype = q_get_disk_file_format(filebytes, size);
    LOG("q_ipc_create_sym ftype %d\n", ftype);
    switch (ftype) {
        case Q_DISK_REGULAR:
            {
                if (size < 9) {
                    return -1;
                }

                // Read the data from the input bytes
                unsigned char type = filebytes[2];
                unsigned char attr = filebytes[3];
                int veclen = q_get_le(KI, filebytes + 4);
                const unsigned char* data = filebytes + 8;
                int datasize = size-8;

                // compute correct veclen.. will be different for different
                // types. the veclen in the file isn't reliable, sadly
                if (type == KS) {
                    veclen = 0;
                    for(int i=0; i<datasize; ++i) {
                        if(data[i] == 0) {
                            veclen++;
                        }
                    }
                }

                LOG("q_ipc_create_sym reg file type=%d, attr=%d, veclen=%d, datasize=%d\n",
                        type, attr, veclen, datasize);

                // Transform to the IPC format understood by d9()
                int ipc_buffer_size = 14+datasize;
                (*kbytes) = ktn(KG, ipc_buffer_size);
                memset(kG(*kbytes), 0, ipc_buffer_size);
                kG(*kbytes)[0] = Q_FILE_LITTLE_ENDIAN;
                q_set_le(KI, kG(*kbytes)+4, ipc_buffer_size);
                kG(*kbytes)[8] = type;
                kG(*kbytes)[9] = attr;
                q_set_le(KI, kG(*kbytes)+10, veclen);
                memcpy(kG(*kbytes)+14, data, datasize);

                LOG("q_ipc_create_sym reg file buffsize=%d msgsize=%d\n",
                        ipc_buffer_size, ipc_buffer_size);

                return 0;
            }
            break;
    }
    return -1;
}

int q_get_le(int ktype, const unsigned char* x) {
    if(ktype == KI) {
        return (x[0]) |
            (x[1] << 8) |
            (x[2] << 16) |
            (x[3] << 24);
    }
    return 0;
}

void q_set_le(int ktype, G* x, int y) {
    if(ktype == KI) {
        x[0] = (y) & 0xFF;
        x[1] = (y >> 8) & 0xFF;
        x[2] = (y >> 16) & 0xFF;
        x[3] = (y >> 24) & 0xFF;
    }
}
