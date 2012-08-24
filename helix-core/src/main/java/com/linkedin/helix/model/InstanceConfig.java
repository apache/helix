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

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixProperty;
import com.linkedin.helix.ZNRecord;

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
    HELIX_DISABLED_PARTITION
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

  public boolean getInstanceEnabled()
  {
    String isEnabled = _record.getSimpleField(InstanceConfigProperty.HELIX_ENABLED.toString());
    return Boolean.parseBoolean(isEnabled);
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

  public Map<String, String> getDisabledPartitionMap()
  {
    return _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString());
  }

//  public void setInstanceEnabledForPartition(String partition, boolean enabled)
//  {
//    if (_record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString()) == null)
//    {
//      _record.setMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString(),
//                             new TreeMap<String, String>());
//    }
//    if (enabled == true)
//    {
//      _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString()).remove(partition);
//    }
//    else
//    {
//      _record.getMapField(InstanceConfigProperty.HELIX_DISABLED_PARTITION.toString()).put(partition, Boolean.toString(false));
//    }
//  }

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
    if(getHostName() == null)
    {
      _logger.error("instanceconfig does not have host name. id:" + _record.getId());
      return false;
    }
    if(getPort() == null)
    {
      _logger.error("instanceconfig does not have host port. id:" + _record.getId());
      return false;
    }
    return true;
  }
}
