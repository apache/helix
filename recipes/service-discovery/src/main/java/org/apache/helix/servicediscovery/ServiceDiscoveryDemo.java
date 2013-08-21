package org.apache.helix.servicediscovery;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.helix.servicediscovery.ServiceDiscovery.Mode;

public class ServiceDiscoveryDemo {
  public static void main(String[] args) throws Exception {

    String clusterName = "service-discovery-demo";
    String zkAddress = "localhost:2199";
    String serviceName = "myServiceName";
    int numServices = 5;

    // registration + zk watch
    demo(clusterName, zkAddress, serviceName, numServices, Mode.WATCH);
    // registration + periodic poll
    demo(clusterName, zkAddress, serviceName, numServices, Mode.POLL);
    // only registration + ondemand
    demo(clusterName, zkAddress, serviceName, numServices, Mode.NONE);

  }

  private static void demo(String clusterName, String zkAddress, String serviceName,
      int numServices, Mode mode) throws Exception, UnknownHostException {
    System.out.println("START:Service discovery demo mode:" + mode);
    ServiceDiscovery serviceDiscovery = new ServiceDiscovery(zkAddress, clusterName, mode);
    serviceDiscovery.start();
    int startPort = 12000;
    List<MyService> serviceList = new ArrayList<MyService>();
    System.out.println("\tRegistering service");
    for (int i = 0; i < numServices; i++) {
      String host = InetAddress.getLocalHost().getHostName();
      int port = startPort + i;
      String serviceId = host + "_" + port;
      ServiceMetadata metadata = new ServiceMetadata();
      metadata.setHost(host);
      metadata.setPort(port);
      metadata.setServiceName(serviceName);
      MyService service = new MyService(serviceId, metadata, serviceDiscovery);
      service.start();
      serviceList.add(service);
      System.out.println("\t\t" + serviceId);
    }
    listAvailableServices(serviceDiscovery);
    stopAndStartServices(serviceDiscovery, serviceList, mode);

    for (MyService service : serviceList) {
      serviceDiscovery.deregister(service.getServiceId());
    }
    serviceDiscovery.stop();
    System.out.println("END:Service discovery demo mode:" + mode);
    System.out.println("=============================================");
  }

  /**
   * Randomly stop and start some services, list all the available services.
   * This demonstrates that the list is dynamically updated when services
   * starts/stops
   * @param serviceDiscovery
   * @param serviceList
   * @param mode
   * @throws Exception
   */
  private static void stopAndStartServices(ServiceDiscovery serviceDiscovery,
      List<MyService> serviceList, Mode mode) throws Exception {
    // randomly select some services stop
    int index = ((int) (Math.random() * 1000)) % serviceList.size();
    MyService service = serviceList.get(index);
    String serviceId = service.getServiceId();
    System.out.println("\tDeregistering service:\n\t\t" + serviceId);
    serviceDiscovery.deregister(serviceId);
    switch (mode) {
    case WATCH:
      Thread.sleep(100);// callback should be immediate
      break;
    case POLL:
      System.out.println("\tSleeping for poll interval:" + ServiceDiscovery.DEFAULT_POLL_INTERVAL);
      Thread.sleep(ServiceDiscovery.DEFAULT_POLL_INTERVAL + 1000);
      break;
    case NONE: // no need to wait, it reads on demand
    default:
      break;
    }
    listAvailableServices(serviceDiscovery);
    Thread.sleep(100);
    System.out.println("\tRegistering service:" + serviceId);
    serviceDiscovery.register(serviceId, service.getMetadata());
  }

  private static void listAvailableServices(ServiceDiscovery serviceDiscovery) {
    List<ServiceMetadata> findAllServices = serviceDiscovery.findAllServices();
    System.out.println("\tSERVICES AVAILABLE");
    System.out.printf("\t\t%s \t%s \t\t\t%s\n", "SERVICENAME", "HOST", "PORT");
    for (ServiceMetadata serviceMetadata : findAllServices) {
      System.out.printf("\t\t%s \t%s \t\t%s\n", serviceMetadata.getServiceName(),
          serviceMetadata.getHost(), serviceMetadata.getPort());
    }
  }
}
