package com.linkedin.clustermanager.store;

import org.I0Itec.zkclient.ZkClient;

import com.linkedin.clustermanager.store.zk.ZKClientFactory;
import com.linkedin.clustermanager.store.zk.ZKPropertyStore;

public class PropertyStoreFactory
{
  public static <T extends Object> PropertyStore<T> getZKPropertyStore(String zkAddress, 
                                                                       PropertySerializer<T> serializer,
                                                                       String rootNamespace)
  {
    ZkClient zkClient = ZKClientFactory.<T>create(zkAddress, serializer);

    return new ZKPropertyStore<T>(zkClient, serializer, rootNamespace);
  }
}
