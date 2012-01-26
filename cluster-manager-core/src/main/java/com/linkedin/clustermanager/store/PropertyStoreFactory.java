package com.linkedin.clustermanager.store;

import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.store.file.FilePropertyStore;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

public class PropertyStoreFactory
{
  public static <T extends Object> PropertyStore<T> getZKPropertyStore(String zkAddress, 
        PropertySerializer<T> serializer, String rootNamespace)
  {
    if (zkAddress == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("arguments can't be null");
    }
    
//    ZkConnection zkConn = new ZkConnection(zkAddress);
    return new ZKPropertyStore<T>(new ZkClient(zkAddress), serializer, rootNamespace);
  }

  public static <T extends Object> PropertyStore<T> getFilePropertyStore(
        PropertySerializer<T> serializer, String rootNamespace, PropertyJsonComparator<T> comparator)
  {
    if (comparator == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("arguments can't be null");
    }
    
    FilePropertyStore<T> store = new FilePropertyStore<T>(serializer, rootNamespace, comparator);
    store.start();
    return store;
   
  }

}
