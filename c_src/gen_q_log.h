#ifndef GEN_Q_LOG_H
#define GEN_Q_LOG_H
#include <stdio.h>

#define LOGGING_ENABLED 0

#if LOGGING_ENABLED
#define LOG(F, ...) fprintf(log_file(), F, __VA_ARGS__); fflush(log_file())
#else
#define LOG(F, ...)
#endif

int open_log();
void close_log();
FILE* log_file();

#endif
