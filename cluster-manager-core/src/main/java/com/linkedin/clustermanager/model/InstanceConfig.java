package com.linkedin.clustermanager.model;

public class InstanceConfig
{
  // setting defaults to avoid lot of null checks
  String _hostName = "UNK";
  String _port = "-1";

  public String getHostName()
  {
    return _hostName;
  }

  public void setHostName(String hostName)
  {
    _hostName = hostName;
  }

  public String getPort()
  {
    return _port;
  }

  public void setPort(String port)
  {
    _port = port;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj instanceof InstanceConfig)
    {
      InstanceConfig that = (InstanceConfig) obj;

      if (this._hostName.equals(that._hostName) && _port.equals(that._port))
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
    sb.append(_hostName);
    sb.append(_port);
    return sb.toString().hashCode();
  }
}
