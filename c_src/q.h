#ifndef GEN_Q_Q_H
#define GEN_Q_Q_H
#include "ei.h"
#include "gen_q_work.h"

extern void q_hopen(QWorkHOpen* data);
extern void q_hclose(QWorkHClose* data);
extern void q_apply(QWorkApply* data, QOpts* opts);
extern void q_hkill(QWorkHKill* data);
extern void q_decodebinary(QWorkDecodeBinary* data, QOpts* opts);
extern void q_dbopen(QWorkDbOp* data, QOpts* opts);
extern void q_dbnext(QWorkDbOp* data, QOpts* opts);
extern void q_dbclose(QWorkDbOp* data, QOpts* opts);

#endif
