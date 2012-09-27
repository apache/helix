package com.linkedin.helix;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public interface BaseDataAccessor<T>
{
  public static class Option
  {
    public static int PERSISTENT = 0x1;
    public static int EPHEMERAL   = 0x2;
    public static int PERSISTENT_SEQUENTIAL   = 0x4;
    public static int EPHEMERAL_SEQUENTIAL   = 0x8;
    public static int THROW_EXCEPTION_IFNOTEXIST = 0x10;
    
    public static CreateMode getMode(int options)
    {
      if ( (options & PERSISTENT) > 0)
      {
        return CreateMode.PERSISTENT;
      } else if ( (options & EPHEMERAL) > 0)
      {
        return CreateMode.EPHEMERAL;
      } else if ( (options & PERSISTENT_SEQUENTIAL) > 0)
      {
        return CreateMode.PERSISTENT_SEQUENTIAL;
      } else if ( (options & EPHEMERAL_SEQUENTIAL) > 0)
      {
        return CreateMode.EPHEMERAL_SEQUENTIAL;
      }
      
      return null;
    }
    
    public static boolean isThrowExceptionIfNotExist(int options)
    {
      return (options & THROW_EXCEPTION_IFNOTEXIST) > 0;
    }
  }
  
  /**
   * This will always attempt to create the znode, if it exists it will return false. Will
   * create parents if they do not exist. For performance reasons, it may try to create
   * child first and only if it fails it will try to create parent
   * 
   * @param path
   * @param record
   * @return
   */
  boolean create(String path, T record, int options);

  /**
   * This will always attempt to set the data on existing node. If the znode does not
   * exist it will create it.
   * 
   * @param path
   * @param record
   * @return
   */
  boolean set(String path, T record, int options);

  /**
   * This will attempt to merge with existing data by calling znrecord.merge and if it
   * does not exist it will create it znode
   * 
   * @param path
   * @param record
   * @return
   */
  boolean update(String path, DataUpdater<T> updater, int options);

  
  /**
   * This will remove znode and all it's child nodes if any
   * 
   * @param path
   * @return
   */
  boolean remove(String path, int options);
  
  /**
   * Use it when creating children under a parent node. This will use async api for better
   * performance. If the child already exists it will return false.
   * 
   * @param parentPath
   * @param record
   * @return
   */
  boolean[] createChildren(List<String> paths, List<T> records, int options);

  /**
   * can set multiple children under a parent node. This will use async api for better
   * performance. If this child does not exist it will create it.
   * 
   * @param parentPath
   * @param record
   */
  boolean[] setChildren(List<String> paths, List<T> records, int options);

  /**
   * Can update multiple nodes using async api for better performance. If a child does not
   * exist it will create it.
   * 
   * @param parentPath
   * @param record
   * @return
   */
  boolean[] updateChildren(List<String> paths, List<DataUpdater<T>> updaters, int options);

  /**
   * remove multiple paths using async api. will remove any child nodes if any
   * 
   * @param paths
   * @return
   */
  boolean[] remove(List<String> paths, int options);
  
  /**
   * Get the {@link T} corresponding to the path
   * 
   * @param path
   * @return
   */
  T get(String path, Stat stat, int options);

  /**
   * Get List of {@link T} corresponding to the paths using async api
   * 
   * @param paths
   * @return
   */
  List<T> get(List<String> paths, List<Stat> stats, int options);

  /**
   * Get the children under a parent path using async api
   * 
   * @param path
   * @return
   */
  List<T> getChildren(String parentPath, List<Stat> stats, int options);

  /**
   * Returns the child names given a parent path
   * 
   * @param type
   * @param keys
   * @return
   */
  List<String> getChildNames(String parentPath, int options);

  /**
   * checks if the path exists in zk
   * 
   * @param path
   * @return
   */
  boolean exists(String path, int options);

  /**
   * checks if the all the paths exists
   * 
   * @param paths
   * @return
   */
  boolean[] exists(List<String> paths, int options);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @return
   */
  Stat[] getStats(List<String> paths, int options);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @return
   */
  Stat getStat(String path, int options);
  
  /**
   * Subscribe/Unsubscribe data listener to path
   * 
   * @param path
   * @param listener
   * @return
   */
  void subscribeDataChanges(String path, IZkDataListener listener);
  void unsubscribeDataChanges(String path, IZkDataListener listener);
  
  /**
   * Subscribe/Unsubscribe child listener to path
   * @param path
   * @param listener
   * @return
   */     
  List<String> subscribeChildChanges(String path, IZkChildListener listener);
  void unsubscribeChildChanges(String path, IZkChildListener listener);

  /**
   * reset the cache if any when session expiry happens
   */
  void reset();
}
