#!/bin/bash

k=0
while :
do
  if [[ $k -eq 10 ]]; then
    break
  fi
  echo "generic wait"
  sleep 1
  k=$k+1
done