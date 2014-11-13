#!/bin/bash
readonly FILE=$1
readonly ARCH=$(uname -s)

if [[ "$ARCH" -eq "Linux" ]]
then
    cp c_src/c.o.l64 $FILE
else if [[ "$ARCH" -eq "Darwin" ]]
then
    cp c_src/c.o.m64 $FILE
fi
fi
