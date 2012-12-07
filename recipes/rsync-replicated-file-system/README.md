Near real time rsync replicated file system
===========================================

Quickdemo
=========
This demo starts 3 instances with id's as ```localhost_12001, localhost_12002, localhost_12003```
Each instance stores its files under /tmp/<id>/filestore
``` localhost_12001 ``` is designated as the master and ``` localhost_12002 and localhost_12003``` are the slaves.
Files written to master are replicated to the slaves automatically. In this demo, a.txt and b.txt are written to ```/tmp/localhost_12001/filestore``` and it gets replicated to other folders.
When the master is stopped, ```localhost_12002``` is promoted to master. 
```localhost_12003``` then stops replicating from ```localhost_12001``` and starts replicating from ```localhost_12002```
Files written to new master are replicated to ```localhost_12003```

```
git clone https://git-wip-us.apache.org/repos/asf/incubator-helix.git
cd recipes/rsync-replicated-file-system/
mvn clean install package
cd target/rsync-replicated-file-system-pkg/bin
./quickdemo

```


