package com.linkedin.helix.manager.zk;

public interface HelixDataListener
{
  /**
   * 
   * @param path
   */
  void onDataChange(String path);
  
  /**
   * 
   * @param path
   */
  void onDataCreate(String path);
  
  /**
   * 
   * @param path
   */
  void onDataDelete(String path);
}
