#!/bin/bash

if [ $1 = 'medals' ]; then
  node ./medals.js $2 $3 $4
  exit
fi

echo "Chart '$1' haven't been found!"
