package org.apache.helix.zookeeper.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.zookeeper.zkclient.IZkConnection;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZooKeeperConnection extends ZkTestBase {
  final int count = 100;
  final AtomicInteger[] get_count = {new AtomicInteger(0)};
  CountDownLatch countDownLatch = new CountDownLatch(count*2);
  CountDownLatch countDownLatch2 = new CountDownLatch(count*3);


  /*
  This function tests persist watchers' behavior in {@link org.apache.helix.zookeeper.zkclient.ZkConnection}
  1. Register a persist watcher on a path and create 100 children Znode, edit the ZNode for 100 times.
  Expecting 200 events.
  2. register a one time listener on the path. Make the same change and count the total number of event.
 */

  @Test
  void testPersistWatcher() throws Exception {
    Watcher watcher1 = new PersistWatcher();
    ZkClient zkClient =   new org.apache.helix.zookeeper.impl.client.ZkClient(ZK_ADDR);
    IZkConnection _zk = zkClient.getConnection();
    String path="/testPersistWatcher";
    _zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    // register a persist listener on a path, change the ZNode 100 times, create 100 child ZNode,
    // and expecting 200 events
    _zk.addWatch(path, watcher1, AddWatchMode.PERSISTENT);
    for (int i=0; i<count; ++i) {
      _zk.writeData(path, "datat".getBytes(), -1);
      _zk.create(path+"/c1_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    Assert.assertTrue(countDownLatch.await(50000, TimeUnit.MILLISECONDS));

    // register a one time listener on the path. Count the total number of event.
    // ZK will over write the persist watcher and only trigger event once for child and data change.
    _zk.readData(path, null, true);
    _zk.getChildren(path, true);
    for (int i=0; i<200; ++i) {
      _zk.writeData(path, ("datat"+i).getBytes(), -1);
      _zk.create(path+"/c2_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    System.out.println("testPersistWatcher: rafter register one time listener, original listener received event count: " + get_count[0]);
    // total number of event is 400. We will miss event now
    Assert.assertTrue(TestHelper.verify(() -> {
      return (get_count[0].get() >= 202 & get_count[0].get() < 400);
    }, TestHelper.WAIT_DURATION));
    zkClient.close();
  }

  @Test (dependsOnMethods = "testPersistWatcher")
  void testRecursivePersistWatcherWithOneTimeWatcher() throws Exception {
    // reset counter
    get_count[0].set(0);
    Watcher watcher1 = new PersistRecurWatcher();
    ZkClient zkClient =   new org.apache.helix.zookeeper.impl.client.ZkClient(ZK_ADDR);
    IZkConnection _zk = zkClient.getConnection();
    String path="/testRecursivePersistWatcher";
    _zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    // register a persist listener on a path, change the ZNode 100 times, create 100 child ZNode,
    // and expecting 200 events
    _zk.addWatch(path, watcher1, AddWatchMode.PERSISTENT_RECURSIVE);
    for (int i=0; i<count; ++i) {
      _zk.writeData(path, "datat".getBytes(), -1);
      _zk.create(path+"/c1_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      _zk.create(path+"/c1_" +i + "/c2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    Assert.assertTrue(countDownLatch2.await(50000, TimeUnit.MILLISECONDS));

    // register a one time listener on the path. Count the total number of event.
    // ZK will over write the persist watcher and only trigger event once for child and data change.
    _zk.readData(path, null, true);
    _zk.getChildren(path, true);
    for (int i=0; i<200; ++i) {
      _zk.writeData(path, ("datat"+i).getBytes(), -1);
      _zk.create(path+"/c2_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    System.out.println("testRecursivePersistWatcherWithOneTimeWatcher: after register one time listener, original listener received event count: " + get_count[0]);
    // total number of event is 500. We will miss event now
    Assert.assertTrue(TestHelper.verify(() -> {
      return (get_count[0].get() >= 302 && get_count[0].get() < 500);
    }, TestHelper.WAIT_DURATION));
    zkClient.close();
  }

  class PersistWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
      get_count[0].incrementAndGet();
      countDownLatch.countDown();
    }
  }

  class PersistRecurWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
      get_count[0].incrementAndGet();
      countDownLatch2.countDown();
    }
  }

}