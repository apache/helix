package com.linkedin.helix.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * A resource contains a set of partitions
 */
public class Resource
{
  private static Logger LOG = Logger.getLogger(Resource.class);

  private final String _resourceName;
  private final Map<String, Partition> _partitionMap;
  private String _stateModelDefRef;

  public Resource(String resourceName)
  {
    this._resourceName = resourceName;
    this._partitionMap = new LinkedHashMap<String, Partition>();
  }

  public String getStateModelDefRef()
  {
    return _stateModelDefRef;
  }

  public void setStateModelDefRef(String stateModelDefRef)
  {
    _stateModelDefRef = stateModelDefRef;
  }

  public String getResourceName()
  {
    return _resourceName;
  }

  public Collection<Partition> getPartitions()
  {
    return _partitionMap.values();
  }

  public void addPartition(String partitionName)
  {
    _partitionMap.put(partitionName, new Partition(partitionName));
  }

  public Partition getPartition(String partitionName)
  {
    return _partitionMap.get(partitionName);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("resourceName:").append(_resourceName);
    sb.append(", stateModelDef:").append(_stateModelDefRef);
    sb.append(", partitionStateMap:").append(_partitionMap);
    
    return sb.toString();
  }
}
