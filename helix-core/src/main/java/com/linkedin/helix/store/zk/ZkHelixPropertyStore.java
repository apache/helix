package com.linkedin.helix.store.zk;

import java.util.List;

import org.I0Itec.zkclient.serialize.ZkSerializer;

import com.linkedin.helix.manager.zk.ZkBaseDataAccessor;
import com.linkedin.helix.manager.zk.ZkCacheBaseDataAccessor;

public class ZkHelixPropertyStore<T> extends ZkCacheBaseDataAccessor<T>
{
  public ZkHelixPropertyStore(ZkBaseDataAccessor<T> accessor,
                              String root,
                              List<String> subscribedPaths)
  {
    super(accessor, root, null, subscribedPaths);
  }

  public ZkHelixPropertyStore(String zkAddress,
                              ZkSerializer serializer,
                              String chrootPath,
                              List<String> zkCachePaths)
  {
    super(zkAddress, serializer, chrootPath, null, zkCachePaths);
  }

  public ZkHelixPropertyStore(String zkAddress, ZkSerializer serializer, String chrootPath)
  {
    super(zkAddress, serializer, chrootPath, null, null);
  }
}
