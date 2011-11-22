package com.linkedin.clustermanager.model;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;

public class InstanceConfig extends ZNRecordAndStat
{
//  private final ZNRecord _record;

  public InstanceConfig(ZNRecord record)
  {
    this(record, null);
  }

  public InstanceConfig(ZNRecord record, Stat stat)
  {
    super(record, stat);
//    _record = record;
  }

  public String getHostName()
  {
    return _record.getSimpleField(CMConstants.ZNAttribute.HOST.toString());
  }

  public void setHostName(String hostName)
  {
    _record.setSimpleField(CMConstants.ZNAttribute.HOST.toString(), hostName);
  }

  public String getPort()
  {
    return _record.getSimpleField(CMConstants.ZNAttribute.PORT.toString());
  }

  public void setPort(String port)
  {
    _record.setSimpleField(CMConstants.ZNAttribute.HOST.toString(), port);
  }

  public boolean getEnabled()
  {
    String isEnabled = _record.getSimpleField(InstanceConfigProperty.ENABLED.toString());
    return Boolean.parseBoolean(isEnabled);
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
