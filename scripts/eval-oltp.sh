#! /bin/bash

if [ "$#" -lt 3 ]
then
  echo "Usage: $0 <min-tp-client> <max-tp-clinet> <step> [num-ap] [output-file]"
  exit 1
fi

min=$1
max=$2
step=$3

if [ "$#" -ge 4 ] 
then
  num_ap=$4
else
  num_ap=0
fi

if [ "$#" -ge 5 ] 
then
  filename=$5
else
  if [ $num_ap -eq 0 ]
  then
    filename="oltp-eval-$min-$max-$step.txt"
  else
    filename="oltp-eval-${min}-${max}-${step}-ap-${num_ap}.txt"
  fi
fi

echo "OLTP clients: [$min, $max], step $step"
echo "OLAP clients: $num_ap"
echo "Output file: $filename"

make -j

echo "" > $filename

for ((i=$min; i<=$max; i += $step))
do
  echo "**** OLTP client = $i ****"
  ./scripts/run2.py txn_base.xml vegito "-t 12 --fly $i" ch >> $filename
done
