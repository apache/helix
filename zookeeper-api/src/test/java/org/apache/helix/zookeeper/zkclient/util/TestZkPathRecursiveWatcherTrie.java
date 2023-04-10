package org.apache.helix.zookeeper.zkclient.util;

import org.apache.helix.zookeeper.zkclient.RecursivePersistWatcherListener;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZkPathRecursiveWatcherTrie {
  ZkPathRecursiveWatcherTrie _recursiveWatcherTrie = new ZkPathRecursiveWatcherTrie();

  @Test
  public void testAddRemoveWatcher() {
    System.out.println("");
    _recursiveWatcherTrie.addRecursiveListener("/a/b/c/d", new TestWatcher());
    _recursiveWatcherTrie.addRecursiveListener("/a/b/c/d1", new TestWatcher());
    _recursiveWatcherTrie.addRecursiveListener("/a/b/c/d2", new TestWatcher());
    _recursiveWatcherTrie.addRecursiveListener("/a/b/c/d3", new TestWatcher());
    _recursiveWatcherTrie.addRecursiveListener("/a/b/c/d4", new TestWatcher());

    TestWatcher listenerOnb = new TestWatcher();
    _recursiveWatcherTrie.addRecursiveListener("/a/b", listenerOnb);
    TestWatcher listenerOnb2 = new TestWatcher();
    _recursiveWatcherTrie.addRecursiveListener("/a/b2", listenerOnb2);
    TestWatcher listenerOnf_1 = new TestWatcher();
    _recursiveWatcherTrie.addRecursiveListener("/a/b3/c/d/e/f", listenerOnf_1);
    TestWatcher listenerOnf_2 = new TestWatcher();
    _recursiveWatcherTrie.addRecursiveListener("/a/b3/c/d/e/f", listenerOnf_2);

    System.out.println("1");
    // node f should have 2 listeners
    Assert.assertEquals(
        _recursiveWatcherTrie.getRootNode().getChild("a").getChild("b3").getChild("c").getChild("d")
            .getChild("e").getChild("f").getRecursiveListeners().size(), 2);

    _recursiveWatcherTrie.removeRecursiveListener("/a/b3/c/d/e/f", listenerOnf_1);
    _recursiveWatcherTrie.removeRecursiveListener("/a/b2", listenerOnb2);
    //b2 will be removed. node "a" should have 2 children, b and b3.
    Assert.assertEquals(_recursiveWatcherTrie.getRootNode().getChild("a").getChildren().size(), 2);
    Assert.assertTrue(_recursiveWatcherTrie.getRootNode().getChild("a").getChildren().contains("b3"));
    Assert.assertTrue(_recursiveWatcherTrie.getRootNode().getChild("a").getChildren().contains("b"));
    // path "/a/b3/c/d/e/f still exists with end node "f" has one listener
    Assert.assertEquals(
        _recursiveWatcherTrie.getRootNode().getChild("a").getChild("b3").getChild("c").getChild("d")
            .getChild("e").getChildren().size(), 1);
    Assert.assertEquals(
        _recursiveWatcherTrie.getRootNode().getChild("a").getChild("b3").getChild("c").getChild("d")
            .getChild("e").getChild("f").getRecursiveListeners().size(), 1);

    // removing all listeners of /a/b3/c/d/e/f.
    _recursiveWatcherTrie.removeRecursiveListener("/a/b3/c/d/e/f", listenerOnf_1); // test no op
    _recursiveWatcherTrie.removeRecursiveListener("/a/b3/c/d/e/f", listenerOnf_2);
    // b3 should be removed as well as all children nodes of b3
    Assert.assertEquals(_recursiveWatcherTrie.getRootNode().getChild("a").getChildren().size(), 1);
  }

  class TestWatcher implements RecursivePersistWatcherListener {

    @Override
    public void handleZNodeChange(String dataPath, Watcher.Event.EventType eventType)
        throws Exception {

    }
  }
}
