#!/bin/sh
cd ../../../../../
mvn clean package -Dmaven.test.skip.exec=true
cd -
chmod +x ../../../../../target/helix-core-pkg/bin/*
./helix_random_kill_local_nojrat.test
./analyze-zklog_local.sh ~/zookeeper/server1/data/ > /tmp/helix_perf_`date '+%y%m%d_%H%m%d%s.log'`
ls -ltr /tmp/helix_perf_* | tail -1 |awk '{print $9}'
ls -ltr /tmp/helix_perf_* | tail -1 | awk '{print $9}' | xargs grep latency
