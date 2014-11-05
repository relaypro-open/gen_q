#ifndef GEN_Q_Q_H
#define GEN_Q_Q_H
#include "ei.h"

extern int q_hopen(char *host, int port, char *unpw, int timeout, ei_x_buff *buff);
extern int q_hclose(int handle, ei_x_buff *buff);

#endif
