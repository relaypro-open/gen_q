#ifndef Q2E_H
#define Q2E_H
#include "k.h"
#include "ei.h"
#include "gen_q_work.h"

extern int ei_x_encode_k(ei_x_buff* x, K r, QOpts* opts);
extern int ei_x_encode_ki_val(ei_x_buff* x, int i);
extern int ei_x_encode_kj_val(ei_x_buff* x, long long j);
extern int ei_x_encode_timestamp_as_now(ei_x_buff* x, long long t);
extern int ei_x_encode_datetime_as_now(ei_x_buff* values, double f);
extern int ei_x_encode_kf_val(ei_x_buff* values, double f);
extern int ei_x_encode_kh_val(ei_x_buff* values, short h);

#endif
