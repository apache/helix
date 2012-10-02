package com.linkedin.helix.store;

public interface HelixPropertyListener
{
  /**
   * Invoked on data change
   * 
   * @param path
   */
  void onDataChange(String path);

  /**
   * Invoked on data creation
   * 
   * @param path
   */
  void onDataCreate(String path);

  /**
   * Invoked on data deletion
   * 
   * @param path
   */
  void onDataDelete(String path);
}
