#!/bin/bash

k=0
while :
do
  if [[ $k -eq 10 ]]; then
    break
  fi
  echo "generic wait"
  sleep 60
  k=$k+1
done