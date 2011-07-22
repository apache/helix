package com.linkedin.clustermanager.controller;

import java.io.FileFilter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.zk.ZKClientFactory;

/**
 * Stores the state of the cluster.
 * 
 * Optimizes read performance by reading data only if the data has changed
 * 
 * @author kgopalak
 * 
 */
public class ClusterDataProvider
{

}

class Node<T>
{
  String name;
  Stat stat;
  T data;
  ConcurrentHashMap<String, Node<T>> children;

}

/**
 * Generic class that will read the data given the root path.
 * 
 * @author kgopalak
 * 
 */
class HeirachicalDataHolder<T>
{
  AtomicReference<Node<T>> root;

  /**
   * currentVersion, gets updated when data is read from original source
   */
  AtomicLong currentVersion;
  private final ZkClient _zkClient;
  private final String _rootPath;
  private final FileFilter _filter;

  public HeirachicalDataHolder(ZkClient client, String rootPath,
      FileFilter filter)
  {
    this._zkClient = client;
    this._rootPath = rootPath;
    this._filter = filter;

  }

  public void hasChangeSince(long version)
  {

  }

  public long getVersion()
  {
    return currentVersion.get();
  }

  public void refreshData()
  {
    Node<T> newRoot = new Node<T>();
    Stat newStat = _zkClient.getStat(_rootPath);
//    refreshRecursively(root.get(), newRoot, newStat, _rootPath);

  }

  private void refreshRecursively(Node<T> oldRoot, Stat oldStat, Node<T> newRoot,Stat newStat, String path)
  {
   // check the parent's first
    if(_zkClient.exists(_rootPath)){
    //_zkClient.gets
    }
    
  }
}