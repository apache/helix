/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.model;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixConstants;

/**
 * A resource contains a set of partitions
 */
public class Resource
{
  private static Logger LOG = Logger.getLogger(Resource.class);

  private final String _resourceName;
  private final Map<String, Partition> _partitionMap;
  private String _stateModelDefRef;
  private String _stateModelFactoryName;
  private int _bucketSize = 0;
  private boolean _enableGroupMessage = false;

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

  public void setStateModelFactoryName(String factoryName)
  {
    if (factoryName == null)
    {
      _stateModelFactoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
    } else
    {
      _stateModelFactoryName = factoryName;
    }
  }

  public String getStateModelFactoryname()
  {
    return _stateModelFactoryName;
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

  public int getBucketSize()
  {
    return _bucketSize;
  }
  
  public void setBucketSize(int bucketSize)
  {
    _bucketSize = bucketSize;
  }

  public void setEnableGroupMessage(boolean enable)
  {
    _enableGroupMessage = enable;
  }
  
  public boolean getEnableGroupMessage()
  {
    return _enableGroupMessage;
  }
  
  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("resourceName:").append(_resourceName);
    sb.append(", stateModelDef:").append(_stateModelDefRef);
    sb.append(", bucketSize:").append(_bucketSize);
    sb.append(", partitionStateMap:").append(_partitionMap);

    return sb.toString();
  }
}
