package org.apache.helix.servicediscovery;

public class MyService
{
  private final ServiceDiscovery serviceDiscovery;
  private final ServiceMetadata metadata;
  private final String serviceId;

  public MyService(String serviceId, ServiceMetadata metadata,
      ServiceDiscovery serviceDiscovery)
  {
    this.serviceId = serviceId;
    this.serviceDiscovery = serviceDiscovery;
    this.metadata = metadata;
  }

  public void start() throws Exception
  {
    serviceDiscovery.register(serviceId, metadata);
  }

  public void stop()
  {
    serviceDiscovery.deregister(serviceId);
  }

  public ServiceDiscovery getServiceDiscovery()
  {
    return serviceDiscovery;
  }

  public ServiceMetadata getMetadata()
  {
    return metadata;
  }

  public String getServiceId()
  {
    return serviceId;
  }

}
