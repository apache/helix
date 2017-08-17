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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkClientMonitor {

  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  private ObjectName buildObjectName(String tag, String key) throws MalformedObjectNameException {
    return MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE, tag,
            ZkClientMonitor.MONITOR_KEY, key);
  }

  private ObjectName buildObjectName(String tag, String key, int num)
      throws MalformedObjectNameException {
    return MBeanRegistrar
        .buildObjectName(num, MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE,
            tag, ZkClientMonitor.MONITOR_KEY, key);
  }

  private ObjectName buildPathMonitorObjectName(String tag, String key, String path)
      throws MalformedObjectNameException {
    return MBeanRegistrar
        .buildObjectName(MonitorDomainNames.HelixZkClient.name(), ZkClientMonitor.MONITOR_TYPE, tag,
            ZkClientMonitor.MONITOR_KEY, key, ZkClientPathMonitor.MONITOR_PATH, path);
  }

  @Test
  public void testMBeanRegisteration() throws JMException {
    final String TEST_TAG_1 = "test_tag_1";
    final String TEST_KEY_1 = "test_key_1";

    ZkClientMonitor monitor = new ZkClientMonitor(TEST_TAG_1, TEST_KEY_1);
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1)));
    ZkClientMonitor monitorDuplicate = new ZkClientMonitor(TEST_TAG_1, TEST_KEY_1);
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, 1)));

    monitor.unregister();
    monitorDuplicate.unregister();

    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1)));
    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, 1)));
  }

  @Test
  public void testCounter() throws JMException {
    final String TEST_TAG = "test_tag_3";
    final String TEST_KEY = "test_key_3";

    ZkClientMonitor monitor = new ZkClientMonitor(TEST_TAG, TEST_KEY);
    ObjectName name = buildObjectName(TEST_TAG, TEST_KEY);
    ObjectName rootName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY,
        ZkClientPathMonitor.PredefinedPath.Root.name());
    ObjectName idealStateName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY,
        ZkClientPathMonitor.PredefinedPath.IdealStates.name());
    ObjectName instancesName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY,
        ZkClientPathMonitor.PredefinedPath.Instances.name());
    ObjectName currentStateName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY,
        ZkClientPathMonitor.PredefinedPath.CurrentStates.name());

    monitor.increaseDataChangeEventCounter();
    long eventCount = (long) _beanServer.getAttribute(name, "DataChangeEventCounter");
    Assert.assertEquals(eventCount, 1);

    monitor.recordRead("TEST/IDEALSTATES/myResource", 0, System.currentTimeMillis() - 10);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "ReadCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(idealStateName, "ReadCounter"), 1);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadLatencyGauge.Max") >= 10);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadMaxLatencyGauge") >= 10);

    monitor.recordRead("TEST/INSTANCES/testDB0", 0, System.currentTimeMillis() - 15);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "ReadCounter"), 2);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "ReadCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(idealStateName, "ReadCounter"), 1);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadTotalLatencyCounter") >= 25);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadMaxLatencyGauge") >= 15);

    monitor.recordWrite("TEST/INSTANCES/node_1/CURRENTSTATES/session_1/Resource", 5,
        System.currentTimeMillis() - 10);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(currentStateName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(currentStateName, "WriteBytesCounter"), 5);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "WriteBytesCounter"), 5);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "WriteTotalLatencyCounter") >= 10);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "WriteMaxLatencyGauge") >= 10);
    Assert
        .assertTrue((long) _beanServer.getAttribute(instancesName, "WriteLatencyGauge.Max") >= 10);
    Assert.assertTrue(
        (long) _beanServer.getAttribute(instancesName, "WriteTotalLatencyCounter") >= 10);
    Assert.assertTrue((long) _beanServer.getAttribute(instancesName, "WriteMaxLatencyGauge") >= 10);
  }
}
