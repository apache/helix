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
import javax.management.*;

import org.apache.helix.manager.zk.zookeeper.ZkEventThread;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkClientMonitor {
  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  private ObjectName buildObjectName(String tag, String key, String instance) throws MalformedObjectNameException {
    return ZkClientMonitor.getObjectName(tag, key, instance);
  }

  private ObjectName buildObjectName(String tag, String key, String instance, int num)
      throws MalformedObjectNameException {
    ObjectName objectName = buildObjectName(tag, key, instance);
    if (num > 0) {
      return new ObjectName(String
          .format("%s,%s=%s", objectName.toString(), MBeanRegistrar.DUPLICATE,
              String.valueOf(num)));
    } else {
      return objectName;
    }
  }

  private ObjectName buildPathMonitorObjectName(String tag, String key, String instance, String path)
      throws MalformedObjectNameException {
    return new ObjectName(String
        .format("%s,%s=%s", buildObjectName(tag, key, instance).toString(), ZkClientPathMonitor.MONITOR_PATH, path));
  }

  @Test
  public void testMBeanRegisteration() throws JMException {
    final String TEST_TAG_1 = "test_tag_1";
    final String TEST_KEY_1 = "test_key_1";

    ZkClientMonitor monitor = new ZkClientMonitor(TEST_TAG_1, TEST_KEY_1, null, true, null);
    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, null)));
    monitor.register();
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, null)));

    // no per-path monitor items created since "monitorRootPathOnly" = true
    Assert.assertFalse(_beanServer.isRegistered(
        buildPathMonitorObjectName(TEST_TAG_1, TEST_KEY_1, null,
            ZkClientPathMonitor.PredefinedPath.IdealStates.name())));

    ZkClientMonitor monitorDuplicate = new ZkClientMonitor(TEST_TAG_1, TEST_KEY_1, null, true, null);
    monitorDuplicate.register();
    Assert.assertTrue(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, null, 1)));

    monitor.unregister();
    monitorDuplicate.unregister();

    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, null)));
    Assert.assertFalse(_beanServer.isRegistered(buildObjectName(TEST_TAG_1, TEST_KEY_1, null, 1)));
  }

  @Test
  public void testCounter() throws JMException {
    final String TEST_TAG = "test_tag_3";
    final String TEST_KEY = "test_key_3";
    final String TEST_INSTANCE = "test_instance_3";

    ZkClientMonitor monitor = new ZkClientMonitor(TEST_TAG, TEST_KEY, TEST_INSTANCE, false, null);
    monitor.register();

    ObjectName name = buildObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE);
    ObjectName rootName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY,
        TEST_INSTANCE, ZkClientPathMonitor.PredefinedPath.Root.name());
    ObjectName idealStateName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
        ZkClientPathMonitor.PredefinedPath.IdealStates.name());
    ObjectName instancesName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
        ZkClientPathMonitor.PredefinedPath.Instances.name());
    ObjectName currentStateName = buildPathMonitorObjectName(TEST_TAG, TEST_KEY, TEST_INSTANCE,
        ZkClientPathMonitor.PredefinedPath.CurrentStates.name());

    monitor.increaseDataChangeEventCounter();
    long eventCount = (long) _beanServer.getAttribute(name, "DataChangeEventCounter");
    Assert.assertEquals(eventCount, 1);

    monitor.increaseStateChangeEventCounter();
    long stateChangeCount = (long) _beanServer.getAttribute(name, "StateChangeEventCounter");
    Assert.assertEquals(stateChangeCount, 1);

    monitor.increaseOutstandingRequestGauge();
    long requestGauge = (long) _beanServer.getAttribute(name, "OutstandingRequestGauge");
    Assert.assertEquals(requestGauge, 1);

    monitor.decreaseOutstandingRequestGauge();
    requestGauge = (long) _beanServer.getAttribute(name, "OutstandingRequestGauge");
    Assert.assertEquals(requestGauge, 0);

    Assert.assertNull(_beanServer.getAttribute(name, "PendingCallbackGauge"));

    monitor.record("TEST/IDEALSTATES/myResource", 0, System.currentTimeMillis() - 10,
        ZkClientMonitor.AccessType.READ);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "ReadCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(idealStateName, "ReadCounter"), 1);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadLatencyGauge.Max") >= 10);
    monitor.record("TEST/INSTANCES/testDB0", 0, System.currentTimeMillis() - 15,
        ZkClientMonitor.AccessType.READ);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "ReadCounter"), 2);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "ReadCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(idealStateName, "ReadCounter"), 1);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "ReadTotalLatencyCounter") >= 25);

    monitor.record("TEST/INSTANCES/node_1/CURRENTSTATES/session_1/Resource", 5,
        System.currentTimeMillis() - 10, ZkClientMonitor.AccessType.WRITE);
    Assert.assertEquals((long) _beanServer.getAttribute(rootName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(currentStateName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(currentStateName, "WriteBytesCounter"), 5);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "WriteCounter"), 1);
    Assert.assertEquals((long) _beanServer.getAttribute(instancesName, "WriteBytesCounter"), 5);
    Assert.assertTrue((long) _beanServer.getAttribute(rootName, "WriteTotalLatencyCounter") >= 10);
    Assert
        .assertTrue((long) _beanServer.getAttribute(instancesName, "WriteLatencyGauge.Max") >= 10);
    Assert.assertTrue(
        (long) _beanServer.getAttribute(instancesName, "WriteTotalLatencyCounter") >= 10);
  }
}
