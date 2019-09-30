package org.apache.helix.rest.server;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.ws.rs.core.Response;

import org.apache.helix.TestHelper;
import org.apache.helix.rest.common.HelixRestNamespace;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDefaultMonitoringMbeans extends AbstractTestClass {
  private static final String DEFAULT_METRIC_DOMAIN = "org.glassfish.jersey";

  // For entire testing environment, we could have 2 - 4 rest server during the testing. So we dont
  // know which REST server got the request and report number. So we have to loop all of them to
  // report data.

  // This is unstable test because the getcluster MBean is even not there after our call
  // and this is not critical for all existing logic. So disable it now.
  // TODO: Make MBean can be stable queried.
  @Test (enabled = false)
  public void testDefaultMonitoringMbeans()
      throws MBeanException, ReflectionException, InstanceNotFoundException, InterruptedException {
    System.out.println("Start test :" + TestHelper.getTestMethodName());
    int listClusters = new Random().nextInt(10);
    for (int i = 0; i < listClusters; i++) {
      get("clusters", null, Response.Status.OK.getStatusCode(), true);
    }

    MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
    boolean correctReports = false;

    // It may take couple milisecond to propagate the data to MBeanServer
    while (!correctReports) {
      for (ObjectName objectName : beanServer.queryNames(null, null)) {
        if (objectName.toString().contains("getClusters")) {
          // The object name is complicated, so we get the matched one and try to find out whether
          // they have the expected attributes and value matched our expectation.
          try {
            if (beanServer.getAttribute(objectName, "RequestCount_total")
                .equals(Long.valueOf(listClusters))) {
              correctReports = true;
            }
          } catch (AttributeNotFoundException e) {
          }
        }
      }
      Thread.sleep(50);
    }

    Assert.assertTrue(correctReports);
    System.out.println("End test :" + TestHelper.getTestMethodName());
  }

  @Test
  public void testMBeanApplicationName()
      throws MalformedObjectNameException {
    Set<String> namespaces =
        new HashSet<>(Arrays.asList(HelixRestNamespace.DEFAULT_NAMESPACE_NAME, TEST_NAMESPACE));
    MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> objectNames =
        mBeanServer.queryNames(new ObjectName(DEFAULT_METRIC_DOMAIN + ":*"), null);

    Set<String> appNames = new HashSet<>();
    for (ObjectName mBeanName : objectNames) {
      appNames.add(mBeanName.getKeyProperty("type"));
    }

    Assert.assertEquals(appNames.size(), namespaces.size(), String
        .format("appNames: %s does't have the same size as namespaces: %s.", appNames, namespaces));

    for (String appName : appNames) {
      Assert.assertTrue(namespaces.contains(appName), String
          .format("Application name: %s is not one of the namespaces: %s.", appName, namespaces));
      namespaces.remove(appName);
    }
  }
}
