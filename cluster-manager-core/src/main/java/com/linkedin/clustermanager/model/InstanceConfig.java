package com.linkedin.clustermanager.model;

public class InstanceConfig
{
  String _hostName;
  String _port;
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
}
