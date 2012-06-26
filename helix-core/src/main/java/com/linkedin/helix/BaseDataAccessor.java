package com.linkedin.helix;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public interface BaseDataAccessor
{
  class Option
  {
    public static int PERSISTENT = 0x1;
    public static int EPHEMERAL   = 0x10;
    public static int PERSISTENT_SEQUENTIAL   = 0x100;
    public static int EPHEMERAL_SEQUENTIAL   = 0x1000;
    
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
  boolean create(String path, ZNRecord record, int options);

  /**
   * This will always attempt to set the data on existing node. If the znode does not
   * exist it will create it.
   * 
   * @param path
   * @param record
   * @return
   */
  boolean set(String path, ZNRecord record, int options);

  /**
   * This will attempt to merge with existing data by calling znrecord.merge and if it
   * does not exist it will create it znode
   * 
   * @param path
   * @param record
   * @return
   */
  boolean update(String path, ZNRecord record, int options);

  /**
   * This will remove znode and all it's child nodes if any
   * 
   * @param path
   * @return
   */
  boolean remove(String path);
  
  /**
   * Use it when creating children under a parent node. This will use async api for better
   * performance. If the child already exists it will return false.
   * 
   * @param parentPath
   * @param record
   * @return
   */
  boolean[] createChildren(List<String> paths, List<ZNRecord> records, int options);

  /**
   * can set multiple children under a parent node. This will use async api for better
   * performance. If this child does not exist it will create it.
   * 
   * @param parentPath
   * @param record
   */
  boolean[] setChildren(List<String> paths, List<ZNRecord> records, int options);

  /**
   * Can update multiple nodes using async api for better performance. If a child does not
   * exist it will create it.
   * 
   * @param parentPath
   * @param record
   * @return
   */
  boolean[] updateChildren(List<String> parentPath, List<ZNRecord> records, int options);

  /**
   * remove multiple paths using async api. will remove any child nodes if any
   * 
   * @param paths
   * @return
   */
  boolean[] remove(List<String> paths);
  
  /**
   * Get the {@link ZNRecord} corresponding to the path
   * 
   * @param path
   * @return
   */
  ZNRecord get(String path, Stat stat, int options);

  /**
   * Get List of {@link ZNRecord} corresponding to the paths using async api
   * 
   * @param paths
   * @return
   */
  List<ZNRecord> get(List<String> paths, int options);

  /**
   * Get the children under a parent path using async api
   * 
   * @param path
   * @return
   */
  List<ZNRecord> getChildren(String parentPath, int options);

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
  boolean exists(String path);

  /**
   * checks if the all the paths exists
   * 
   * @param paths
   * @return
   */
  boolean[] exists(List<String> paths);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @return
   */
  Stat[] getStats(List<String> paths);

  /**
   * Get the stats of all the paths
   * 
   * @param paths
   * @return
   */
  Stat getStat(String path);
  
  /**
   * Subscribe listener to path
   * 
   * @param path
   * @param listener
   * @return
   */
  boolean subscribe(String path, IZkListener listener);
  
  /**
   * Unsubscribe listener to path
   * @param path
   * @param listener
   * @return
   */     
  boolean unsubscribe(String path, IZkListener listener);
}
