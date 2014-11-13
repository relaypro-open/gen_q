#!/bin/bash
readonly FILE=$1
readonly ARCH=$(uname -s)

if [[ "$ARCH" == "Linux" ]]
then
    echo "Copying l64 for arch $ARCH"
    cp c_src/c.o.l64 $FILE
else if [[ "$ARCH" == "Darwin" ]]
then
    echo "Copying m64 for arch $ARCH"
    cp c_src/c.o.m64 $FILE
fi
fi
