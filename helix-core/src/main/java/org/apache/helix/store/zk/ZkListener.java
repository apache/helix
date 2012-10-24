package org.apache.helix.store.zk;

public interface ZkListener
{
  void handleDataChange(String path);
  
  void handleNodeCreate(String path);
  
  void handleNodeDelete(String path);
}
