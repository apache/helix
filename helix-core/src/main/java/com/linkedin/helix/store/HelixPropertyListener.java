package com.linkedin.helix.store;

public interface HelixPropertyListener
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
