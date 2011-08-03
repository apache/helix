package com.linkedin.clustermanager.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ResourceGroup
{

  private final String _resourceGroupId;

  private final Map<String, ResourceKey> _resourceKeyMap;

  public ResourceGroup(String resourceGroupId)
  {
    this._resourceGroupId = resourceGroupId;
    
    this._resourceKeyMap = new LinkedHashMap<String, ResourceKey>();
  }

  public Collection<ResourceKey> getResourceKeys()
  {
    return _resourceKeyMap.values();
  }

  public void addResource(String resourceKey)
  {
    _resourceKeyMap.put(resourceKey, new ResourceKey(resourceKey));
  }

  public ResourceKey getResourceKey(String resourceKeyStr)
  {
    return _resourceKeyMap.get(resourceKeyStr);
  }

}
