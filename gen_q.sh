#!/bin/bash

APP=gen_q
OPT_HOME=${OPT_HOME:=/opt/$APP}
OPT_DB_DIR=${OPT_DB_DIR:=/var/db/$APP}
OPT_LOG_DIR=${OPT_LOG_DIR:=/var/log/$APP}
OPT_PID_DIR=${OPT_PID_DIR:=/var/run/$APP}
OPT_USER=${OPT_USER:=`id -u -n`}

mkdir -p $OPT_PID_DIR

echo "Using OPT_HOME $OPT_HOME"
echo "Using OPT_DB_DIR $OPT_DB_DIR"
echo "Using OPT_LOG_DIR $OPT_LOG_DIR"
echo "Using OPT_PID_DIR $OPT_PID_DIR"

HOST=`hostname -s`

ERL_VER=`erl +V 2>&1 | awk '{print $6}' | sed "s/$(printf '\r')//"`

SSL_OPTS="-proto_dist inet_tls -ssl_dist_opt server_certfile ${OPT_HOME}/priv/certs/server.pem -ssl_dist_opt server_verify verify_none -ssl_dist_opt server_depth 0 -ssl_dist_opt server_fail_if_no_peer_cert false -ssl_dist_opt client_certfile ${OPT_HOME}/priv/certs/client.pem -ssl_dist_opt client_verify verify_none -ssl_dist_opt client_depth 0"

ERL_LIBS=${ERL_LIBS}:${OPT_HOME}:${OPT_HOME}/libs:${OPT_HOME}/deps

export ERL_LIBS=${ERL_LIBS}

ENV_VARS="ERL_MAX_ETS_TABLES 4096"

BC_CEIL="define ceil(x) { auto savescale; savescale = scale; scale = 0; if (x>0) { if (x%1>0) result = x+(1-(x%1)) else result = x } else result = -1*floor(-1*x);  scale = savescale; return result };"

which nproc 1>/dev/null 2>/dev/null && SCHEDULERS_ADJUSTMENT=1
SCHEDULERS_ADJUSTMENT=${SCHEDULERS_ADJUSTMENT:-0}

if [ "$SCHEDULERS_ADJUSTMENT" -eq 1 ]
then
    NPROCS=`nproc`
    CPUS=`echo -e $BC_CEIL"(ceil((0.8 * $NPROCS)) + 0.5)/1" | bc`
    SCHEDULERS="+S $CPUS:$CPUS"
else
    SCHEDULERS=""
fi

start() {
   echo "Starting $APP"
   NODE="$APP"
   erl 128 +P 4194304 $SCHEDULERS +A 128 +K true \
	   -noinput -detached -mnesia dir \"${OPT_DB_DIR}\" \
       -boot ${OPT_HOME}/priv/$APP"_"${ERL_VER} \
       -env $ENV_VARS \
       -sname $NODE ${SSL_OPTS} -config ${OPT_HOME}/$APP -s $APP
}

console() {
   echo "Starting $APP in console mode"
   NODE="$APP@$HOST"
   erl 128 +P 4194304 $SCHEDULERS +A 128 +K true \
	   -mnesia dir \"${OPT_DB_DIR}\" \
       -boot ${OPT_HOME}/priv/$APP"_"${ERL_VER} \
       -env $ENV_VARS \
       -sname $NODE ${SSL_OPTS} -config ${OPT_HOME}/$APP -s $APP
}

stop() {
    echo "Stopping $APP"
    NODE=$APP"ctl@$HOST"
    erl -noshell \
       -boot ${OPT_HOME}/priv/$APP"_"${ERL_VER} \
       -sname $NODE ${SSL_OPTS} -s $APP stop
}

debug() {
    NODE=$APP"debug_${OPT_USER}"
    erl -sname $NODE ${SSL_OPTS} \
        -boot ${OPT_HOME}/priv/$APP"_"${ERL_VER} \
        -setcookie `cat /home/${OPT_USER}/.erlang.cookie` \
        -remsh $APP@$HOST
}

if [ $# -eq 1 ]
then
    case $1 in
        "start" ) start ;;
        "stop"  ) stop ;;
        "debug" ) debug ;;
        "console" ) console ;;
        *       ) echo "Invalid command: $1";;
    esac
else
    echo "Invalid"
fi
