package com.linkedin.helix.store;

import com.linkedin.helix.BaseDataAccessor;

public interface HelixPropertyStore<T> extends BaseDataAccessor<T>
{
  /**
   * Perform resource allocation when property store starts
   * 
   * Resource allocation includes: - start an internal thread for fire callbacks
   * 
   */
  public void start();

  /**
   * Perform clean up when property store stops
   * 
   * Cleanup includes: - stop the internal thread for fire callbacks
   * 
   */
  public void stop();

  /**
   * Register a listener to a parent path.
   * 
   * Subscribing to a parent path means any changes happening under the parent path will
   * notify the listener
   * 
   * @param parentPath
   * @param listener
   */
  public void subscribe(String parentPath, HelixPropertyListener listener);

  /**
   * Remove a listener from a parent path.
   * 
   * This will remove the listener from receiving any notifications happening under the
   * parent path
   * 
   * @param parentPath
   * @param listener
   */
  public void unsubscribe(String parentPath, HelixPropertyListener listener);
}
