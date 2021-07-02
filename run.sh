#!/bin/bash

if [ "$#" -lt 2 ]
then
  echo "Usage: $0 <num_thread> <config-name> [benchmark] [on-fly]"
  exit 1
fi


num_thread=$1
config_name=$2

if [ ! $3 ]; then
  benchmark="ch"
else
  benchmark=$3
fi

if [ ! $4 ]; then
  cmd="-t $num_thread"
else
  cmd="-t $num_thread --fly $4"
fi

make -j; ./scripts/run2.py $config_name vegito "$cmd" $benchmark
