#!/bin/bash

if [ ! $1 ] ;then
  echo "No testName provided!
Example: script.sh <testClass[#testMethod]> [repeat-time] [project]"
  exit
fi
testName=$1

if [ ! $2 ] ;then
  repeated=-1
else
  repeated=$2
fi

if [ ! $3 ] ;then
  project="helix-core"
else
  project=$3
fi

echo "Running test on $testName in component $project for $repeated times."
for (( i = 1; $repeated < 0 || i <= $repeated; i++ ))
do
  echo "======================================================================
Attempt $i $testName
======================================================================"
  mvn test -o -Dtest=$testName -pl=$project
  exitcode=$?
  if [ $exitcode -ne 0 ]
  then
    echo "======================================================================
Error at attempt $i
======================================================================"
    break
  fi
done


