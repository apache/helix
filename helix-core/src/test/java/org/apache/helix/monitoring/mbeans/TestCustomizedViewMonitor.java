package org.apache.helix.monitoring.mbeans;

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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Stack;
import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.TestHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCustomizedViewMonitor {
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  private final String TEST_CLUSTER = "test_cluster";
  private final String MAX_SUFFIX = ".Max";
  private final String MEAN_SUFFIX = ".Mean";

  private ObjectName buildObjectName(int duplicateNum) throws MalformedObjectNameException {
    ObjectName objectName = new ObjectName(String
        .format("%s:%s=%s,%s=%s", MonitorDomainNames.AggregatedView.name(), "Type",
            "CustomizedView", "Cluster", TEST_CLUSTER));
    if (duplicateNum == 0) {
      return objectName;
    } else {
      return new ObjectName(
          String.format("%s,%s=%s", objectName.toString(), MBeanRegistrar.DUPLICATE, duplicateNum));
    }
  }

  @Test
  public void testMBeanRegistration() throws JMException, IOException {
    int numOfMonitors = 5;

    Stack<CustomizedViewMonitor> monitors = new Stack<>();
    for (int i = 0; i < numOfMonitors; i++) {
      CustomizedViewMonitor monitor = new CustomizedViewMonitor(TEST_CLUSTER);
      monitor.register();
      monitors.push(monitor);
    }

    for (int i = 0; i < numOfMonitors; i++) {
      if (i == numOfMonitors - 1) {
        Assert.assertTrue(_server.isRegistered(buildObjectName(0)));
      } else {
        Assert.assertTrue(_server.isRegistered(buildObjectName(numOfMonitors - i - 1)));
      }
      CustomizedViewMonitor monitor = monitors.pop();
      assert monitor != null;
      monitor.unregister();
      if (i == numOfMonitors - 1) {
        Assert.assertFalse(_server.isRegistered(buildObjectName(0)));
      } else {
        Assert.assertFalse(_server.isRegistered(buildObjectName(numOfMonitors - i - 1)));
      }
    }
  }

  @Test
  public void testMetricInitialization() throws Exception {
    CustomizedViewMonitor monitor = new CustomizedViewMonitor(TEST_CLUSTER);
    monitor.register();
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      monitor.recordUpdateToAggregationLatency(i);
      sum += i;
      int expectedMax = i;
      double expectedMean = sum / (i + 1.0);
      Assert.assertTrue(TestHelper.verify(() -> (long) _server.getAttribute(buildObjectName(0),
          CustomizedViewMonitor.UPDATE_TO_AGGREGATION_LATENCY_GAUGE + MAX_SUFFIX) == expectedMax,
          TestHelper.WAIT_DURATION));
      Assert.assertTrue(TestHelper.verify(() -> (double) _server.getAttribute(buildObjectName(0),
          CustomizedViewMonitor.UPDATE_TO_AGGREGATION_LATENCY_GAUGE + MEAN_SUFFIX) == expectedMean,
          TestHelper.WAIT_DURATION));
    }
    monitor.unregister();
  }
}
