#!/bin/bash
# command line interface to cluster manager management interface

echo $@
ESPRESSO_HOME="../../../" 
#
#TODO: find out how to obtain the classpaths. Ideally from ant.
ant -f $ESPRESSO_HOME/cluster-manager/run/build.xml run-cm-console -Dconfig.cmdline=\"$@\" 
