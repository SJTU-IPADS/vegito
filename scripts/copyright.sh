#!/bin/bash

copyright=~/htap/copyright.txt

for i in *.cc # or whatever other pattern...
do
  if ! grep -q Copyright $i
  then
    echo $i
    if [ "$1" == "c" ]; then
      cat $copyright $i >$i.new && mv $i.new $i
    fi
   fi
done

for i in *.h # or whatever other pattern...
do
  if ! grep -q Copyright $i
  then
    echo $i
    if [ "$1" == "c" ]; then
      cat $copyright $i >$i.new && mv $i.new $i
    fi
   fi
done

# rm *.new
