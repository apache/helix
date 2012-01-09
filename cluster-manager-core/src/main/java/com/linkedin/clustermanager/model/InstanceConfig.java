package com.linkedin.clustermanager.model;

import java.util.Map;
import java.util.TreeMap;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

/**
 * Instance configurations
 */
public class InstanceConfig extends ZNRecordDecorator
{
  public enum InstanceConfigProperty
  {
    HOST,
    PORT,
    ENABLED,
    DISABLED_PARTITION
  }

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
    return _record.getSimpleField(InstanceConfigProperty.HOST.toString());
  }

  public void setHostName(String hostName)
  {
    _record.setSimpleField(InstanceConfigProperty.HOST.toString(), hostName);
  }

  public String getPort()
  {
    return _record.getSimpleField(InstanceConfigProperty.PORT.toString());
  }

  public void setPort(String port)
  {
    _record.setSimpleField(InstanceConfigProperty.PORT.toString(), port);
  }

  public boolean getInstanceEnabled()
  {
    String isEnabled = _record.getSimpleField(InstanceConfigProperty.ENABLED.toString());
    return Boolean.parseBoolean(isEnabled);
  }

  public void setInstanceEnabled(boolean enabled)
  {
    _record.setSimpleField(InstanceConfigProperty.ENABLED.toString(), Boolean.toString(enabled));
  }


  public boolean getInstanceEnabledForResource(String resource)
  {
    Map<String, String> disabledPartitionMap = _record.getMapField(InstanceConfigProperty.DISABLED_PARTITION.toString());
    if (disabledPartitionMap != null && disabledPartitionMap.containsKey(resource))
    {
      return false;
    }
    else
    {
      return true;
    }
  }

  public void setInstanceEnabledForResource(String resource, boolean enabled)
  {
    if (_record.getMapField(InstanceConfigProperty.DISABLED_PARTITION.toString()) == null)
    {
      _record.setMapField(InstanceConfigProperty.DISABLED_PARTITION.toString(),
                             new TreeMap<String, String>());
    }
    if (enabled == true)
    {
      _record.getMapField(InstanceConfigProperty.DISABLED_PARTITION.toString()).remove(resource);
    }
    else
    {
      _record.getMapField(InstanceConfigProperty.DISABLED_PARTITION.toString()).put(resource, Boolean.toString(false));
    }
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
}
