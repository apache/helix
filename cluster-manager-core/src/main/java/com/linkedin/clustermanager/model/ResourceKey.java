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

  @Override
  public boolean equals(Object obj)
  {
    return _resourceKeyName.equals(obj);
  }

  @Override
  public int hashCode()
  {
    return _resourceKeyName.hashCode();
  }
}
