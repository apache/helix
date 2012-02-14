package com.linkedin.helix.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.participant.HelixStateMachineEngine;

/**
 * A resource group contains a number of resources
 */
public class ResourceGroup
{
  private static Logger LOG = Logger.getLogger(ResourceGroup.class);

  private final String _resourceGroupId;
  private final Map<String, ResourceKey> _resourceKeyMap;
  private String _stateModelDefRef;
  private final String _stateModelFactoryName;

  public ResourceGroup(String resourceGroupName)
  {
    this(resourceGroupName, HelixStateMachineEngine.DEFAULT_FACTORY);
  }

  public ResourceGroup(String resourceGroupId, String factoryName)
  {
    this._resourceGroupId = resourceGroupId;
    this._stateModelFactoryName = factoryName;
    this._resourceKeyMap = new LinkedHashMap<String, ResourceKey>();
  }

  public String getStateModelFactoryName()
  {
    return _stateModelFactoryName;
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

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("resourceGroupId:").append(_resourceGroupId);
    sb.append(", stateModelDef:").append(_stateModelDefRef);
    sb.append(", resourceKeyMap:").append(_resourceKeyMap);

    return sb.toString();
  }
}
