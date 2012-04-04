package com.linkedin.helix.store;

import org.apache.log4j.Logger;

import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.file.FilePropertyStore;
import com.linkedin.helix.store.zk.ZKPropertyStore;

public class PropertyStoreFactory
{
  private static Logger LOG = Logger.getLogger(PropertyStoreFactory.class);

  public static <T extends Object> PropertyStore<T> getZKPropertyStore(String zkAddress,
        PropertySerializer<T> serializer, String rootNamespace)
  {
    if (zkAddress == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("arguments can't be null");
    }

    LOG.info("Get a zk property store. zkAddr: " + zkAddress + ", root: " + rootNamespace);
    return new ZKPropertyStore<T>(new ZkClient(zkAddress), serializer, rootNamespace);
  }

  public static <T extends Object> PropertyStore<T> getFilePropertyStore(
        PropertySerializer<T> serializer, String rootNamespace, PropertyJsonComparator<T> comparator)
  {
    if (comparator == null || serializer == null || rootNamespace == null)
    {
      throw new IllegalArgumentException("arguments can't be null");
    }

    LOG.info("Get a file property store. root: " + rootNamespace);
    FilePropertyStore<T> store = new FilePropertyStore<T>(serializer, rootNamespace, comparator);
    return store;

  }

}
