package com.linkedin.clustermanager.model;

public class ResourceKey
{

  private final String _resourceKeyName;

  public String getResourceKeyName()
  {
    return _resourceKeyName;
  }

  public ResourceKey(String resourceKey)
  {
    this._resourceKeyName = resourceKey;
  }
  
}
