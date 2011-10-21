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
    if(obj == null || !(obj instanceof ResourceKey)){
      return false;
    }
    
    ResourceKey that = (ResourceKey)obj;
    return _resourceKeyName.equals(that.getResourceKeyName());
  }

  @Override
  public int hashCode()
  {
    return _resourceKeyName.hashCode();
  }
  @Override
  public String toString()
  {
    return _resourceKeyName;
  }
}
