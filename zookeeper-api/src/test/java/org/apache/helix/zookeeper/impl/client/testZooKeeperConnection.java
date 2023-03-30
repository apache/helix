package org.apache.helix.zookeeper.impl.client;

import java.io.IOException;

import org.apache.helix.zookeeper.impl.ZkTestBase;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.annotations.Test;


public class testZooKeeperConnection extends ZkTestBase {


  @Test
  void testPersistWatcher() throws IOException, KeeperException, InterruptedException {
    Watcher watcher1 = new PersistWatcher();
    ZooKeeper _zk = new ZooKeeper(ZkTestBase.ZK_ADDR, 30 * 1000, watcher1);
    _zk.create("/root", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    //_zk.getChildren("/root", true);
    _zk.addWatch("/root", watcher1, AddWatchMode.PERSISTENT);
    _zk.create("/root/1", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    _zk.create("/root/2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    _zk.create("/root/3", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    _zk.create("/root/4", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  class PersistWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
      System.out.println("path: " + watchedEvent.getPath());
      System.out.println("type: " + watchedEvent.getType());
    }
  }
}
