package com.linkedin.helix.store;

import com.linkedin.helix.BaseDataAccessor;

public interface HelixPropertyStore<T> extends BaseDataAccessor<T>
{
  public void start();

  public void stop();
  
  public void subscribe(String parentPath, HelixPropertyListener listener);
  
  public void unsubscribe(String parentPath, HelixPropertyListener listener);
}
