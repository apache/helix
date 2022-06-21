#!/bin/bash

basedir=`dirname $0`/../
outdir=helix-front-pkg

cd ${basedir}/target

rm -rf ${outdir}
mkdir ${outdir}

tar xf helix-front-*.tar -C ${outdir} --strip-components 1

