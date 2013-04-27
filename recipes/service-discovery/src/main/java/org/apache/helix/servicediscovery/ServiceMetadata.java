package org.apache.helix.servicediscovery;

public class ServiceMetadata
{

  private int _port;

  private String _host;

  public String getHost()
  {
    return _host;
  }

  public void setHost(String host)
  {
    this._host = host;
  }

  private String _serviceName;

  public ServiceMetadata()
  {

  }

  public void setPort(int port)
  {
    _port = port;
  }

  public int getPort()
  {
    return _port;
  }

  public String getServiceName()
  {
    return _serviceName;
  }

  public void setServiceName(String serviceName)
  {
    this._serviceName = serviceName;
  }

}
