package org.apache.helix.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;


/**
 * Instance configurations
 */
public class InstanceConfig extends HelixProperty
{
  public enum InstanceConfigProperty
  {
    HELIX_HOST,
    HELIX_PORT,
    HELIX_ENABLED,
    HELIX_DISABLED_PARTITION,
    TAG_LIST
  }
  private static final Logger _logger = Logger.getLogger(InstanceConfig.class.getName());

  public InstanceConfig(String id)
  {
    super(id);
  }

  public InstanceConfig(ZNRecord record)
  {
    super(record);
  }

  public String getHostName()
  {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_HOST.toString());
  }

  public void setHostName(String hostName)
  {
    _record.setSimpleField(InstanceConfigProperty.HELIX_HOST.toString(), hostName);
  }

  public String getPort()
  {
    return _record.getSimpleField(InstanceConfigProperty.HELIX_PORT.toString());
  }

  public void setPort(String port)
  {
    _record.setSimpleField(InstanceConfigProperty.HELIX_PORT.toString(), port);
  }
  
  public List<String> getTags()
  {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if(tags == null)
    {
      tags = new ArrayList<String>(0);
    }
    return tags;
  }
  
  public void addTag(String tag)
  {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if(tags == null)
    {
      tags = new ArrayList<String>(0);
    }
    if(!tags.contains(tag))
    {
      tags.add(tag);
    }
    getRecord().setListField(InstanceConfigProperty.TAG_LIST.toString(), tags);
  }
  
  public void removeTag(String tag)
  {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if(tags == null)
    {
      return;
    }
    if(tags.contains(tag))
    {
      tags.remove(tag);
    }
  }
  
  public boolean containsTag(String tag)
  {
    List<String> tags = getRecord().getListField(InstanceConfigProperty.TAG_LIST.toString());
    if(tags == null)
    {
      return false;
    }
    return tags.contains(tag);
  }

  public boolean getInstanceEnabled()
  {
    String isEnabled = _record.getSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString());
    return isEnabled == null || Boolean.parseBoolean(isEnabled);
  }

  public void setInstanceEnabled(boolean enabled)
  {
    _record.setSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString(), Boolean.toString(enabled));
  }


  public boolean getInstanceEnabledForPartition(String partition)
  {
    // Map<String, String> disabledPartitionMap = _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    List<String> disabledPartitions = _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    if (disabledPartitions != null && disabledPartitions.contains(partition))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  public List<String> getDisabledPartitions()
  {
    return _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());

  }

  public void setInstanceEnabledForPartition(String partitionName, boolean enabled)
  {
    List<String> list =
        _record.getListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
    Set<String> disabledPartitions = new HashSet<String>();
    if (list != null)
    {
      disabledPartitions.addAll(list);
    }

    if (enabled)
    {
      disabledPartitions.remove(partitionName);
    }
    else
    {
      disabledPartitions.add(partitionName);
    }

    list = new ArrayList<String>(disabledPartitions);
    Collections.sort(list);
    _record.setListField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(),
                             list);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj instanceof InstanceConfig)
    {
      InstanceConfig that = (InstanceConfig) obj;

      if (this.getHostName().equals(that.getHostName()) && this.getPort().equals(that.getPort()))
      {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode()
  {

    StringBuffer sb = new StringBuffer();
    sb.append(this.getHostName());
    sb.append("_");
    sb.append(this.getPort());
    return sb.toString().hashCode();
  }

  public String getInstanceName()
  {
    return _record.getId();
  }

  @Override
  public boolean isValid()
  {
    // HELIX-65: remove check for hostname/port existence
    return true;
  }
}
