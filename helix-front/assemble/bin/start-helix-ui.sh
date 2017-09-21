#!/bin/bash

basedir=`dirname $0`/../
cd ${basedir}

if [ ! -d node_modules ]; then
  node/node node/npm/bin/npm-cli.js install
fi

node/node dist/server/app.js

