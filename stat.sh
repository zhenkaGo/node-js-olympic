#!/bin/bash

if [ $1 = 'medals' ]; then
  node ./charts/medals.js $2 $3 $4
  exit
fi

if [ $1 = 'top-teams' ]; then
  node ./charts/topTeams.js $2 $3 $4
  exit
fi

echo "Chart '$1' haven't been found!"
