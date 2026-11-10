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


  /*
  This function tests persist watchers' behavior in {@link org.apache.helix.zookeeper.zkclient.ZkConnection}
  1. Register a persist watcher on a path and create 100 children Znode, edit the ZNode for 100 times.
  Expecting 200 events.
  2. register a one time listener on the path. Make the same change and count the total number of event.
  Since ZooKeeper 3.9.0 (ZOOKEEPER-4466) a one time watch on the same path no longer replaces the
  persistent watch on the server, so the persistent watcher keeps receiving every event. Before that
  fix it silently missed events, which is why ZkClient refuses one time watches in persist mode.
 */

  @Test
  void testPersistWatcher() throws Exception {
    CountingWatcher watcher1 = new CountingWatcher(count * 2);
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
    Assert.assertTrue(watcher1.awaitInitialEvents(50000));

    // register a one time listener on the path. Count the total number of event.
    // The one time watch coexists with the persist watcher (ZOOKEEPER-4466), so the persist
    // watcher still gets all 200 data change and 200 child change events.
    _zk.readData(path, null, true);
    _zk.getChildren(path, true);
    for (int i=0; i<200; ++i) {
      _zk.writeData(path, ("datat"+i).getBytes(), -1);
      _zk.create(path+"/c2_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    System.out.println("testPersistWatcher: after register one time listener, original listener received event count: " + watcher1.getCount());
    // total number of events is 200 + 400 = 600 and none of them is missed
    TestHelper.verify(() -> watcher1.getCount() >= 600, TestHelper.WAIT_DURATION);
    Assert.assertEquals(watcher1.getCount(), 600, "Unexpected number of persist watcher events");
    zkClient.close();
  }

  @Test (dependsOnMethods = "testPersistWatcher")
  void testRecursivePersistWatcherWithOneTimeWatcher() throws Exception {
    CountingWatcher watcher1 = new CountingWatcher(count * 3);
    ZkClient zkClient =   new org.apache.helix.zookeeper.impl.client.ZkClient(ZK_ADDR);
    IZkConnection _zk = zkClient.getConnection();
    String path="/testRecursivePersistWatcher";
    _zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    // register a recursive persist listener on a path, change the ZNode 100 times, create 100 child
    // ZNode and 100 grandchild ZNode, and expecting 300 events. A recursive persist watcher only gets
    // NodeDataChanged/NodeCreated/NodeDeleted, no NodeChildrenChanged (ZOOKEEPER-4475).
    _zk.addWatch(path, watcher1, AddWatchMode.PERSISTENT_RECURSIVE);
    for (int i=0; i<count; ++i) {
      _zk.writeData(path, "datat".getBytes(), -1);
      _zk.create(path+"/c1_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      _zk.create(path+"/c1_" +i + "/c2", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    Assert.assertTrue(watcher1.awaitInitialEvents(50000));

    // register a one time listener on the path. Count the total number of event.
    // The one time watch coexists with the persist watcher (ZOOKEEPER-4466), so the persist
    // watcher still gets all 200 data change and 200 child creation events.
    _zk.readData(path, null, true);
    _zk.getChildren(path, true);
    for (int i=0; i<200; ++i) {
      _zk.writeData(path, ("datat"+i).getBytes(), -1);
      _zk.create(path+"/c2_" +i, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    System.out.println("testRecursivePersistWatcherWithOneTimeWatcher: after register one time listener, original listener received event count: " + watcher1.getCount());
    // total number of events is 300 + 400 = 700 and none of them is missed
    TestHelper.verify(() -> watcher1.getCount() >= 700, TestHelper.WAIT_DURATION);
    Assert.assertEquals(watcher1.getCount(), 700,
        "Unexpected number of recursive persist watcher events");
    zkClient.close();
  }

  /*
  Counts the node events of one test. Each test has its own instance, so a late callback of one
  test cannot leak into the count of another one.
  Persist watchers also receive the session state events, for example the Closed event that is
  delivered asynchronously when the client is closed. They are not node events and are not
  counted; anything other than Closed is printed because a lost session may explain missed events.
  */
  static class CountingWatcher implements Watcher {
    private final AtomicInteger _count = new AtomicInteger(0);
    private final CountDownLatch _initialEvents;

    CountingWatcher(int numInitialEvents) {
      _initialEvents = new CountDownLatch(numInitialEvents);
    }

    int getCount() {
      return _count.get();
    }

    boolean awaitInitialEvents(long timeoutMs) throws InterruptedException {
      return _initialEvents.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      if (watchedEvent.getType() == Event.EventType.None) {
        if (watchedEvent.getState() != Event.KeeperState.Closed) {
          System.out.println("CountingWatcher received session event: " + watchedEvent);
        }
        return;
      }
      _count.incrementAndGet();
      _initialEvents.countDown();
    }
  }

}
