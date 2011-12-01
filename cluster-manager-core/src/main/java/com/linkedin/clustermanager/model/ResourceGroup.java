package com.linkedin.clustermanager.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ResourceGroup
{
  private static Logger LOG = Logger.getLogger(ResourceGroup.class);

  private final String _resourceGroupId;
  private final Map<String, ResourceKey> _resourceKeyMap;
  private String _stateModelDefRef;

  public ResourceGroup(String resourceGroupId)
  {
    this._resourceGroupId = resourceGroupId;

    this._resourceKeyMap = new LinkedHashMap<String, ResourceKey>();

  }

  public String getStateModelDefRef()
  {
    return _stateModelDefRef;
  }

  public void setStateModelDefRef(String stateModelDefRef)
  {
    _stateModelDefRef = stateModelDefRef;
  }

  public String getResourceGroupId()
  {
    return _resourceGroupId;
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
