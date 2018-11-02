package org.apache.helix.view.monitoring;

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
import javax.management.ObjectName;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestViewAggregatorMonitor {
  private static final MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  @Test
  public void testViewAggregatorMonitorMBeanRegistration() throws Exception {
    String cluster1 = "cluster1";
    String cluster2 = "cluster2";
    ObjectName name1 = generateObjectName(cluster1);
    ObjectName name2 = generateObjectName(cluster2);

    ViewAggregatorMonitor monitor1 = new ViewAggregatorMonitor(cluster1);
    monitor1.register();

    ViewAggregatorMonitor monitor2 = new ViewAggregatorMonitor(cluster2);
    monitor2.register();

    Assert.assertTrue(_beanServer.isRegistered(name1));
    Assert.assertTrue(_beanServer.isRegistered(name2));

    monitor1.unregister();
    monitor2.unregister();

    Assert.assertFalse(_beanServer.isRegistered(name1));
    Assert.assertFalse(_beanServer.isRegistered(name2));
  }

  @Test
  public void testViewAggregatorMonitorDataRecording() throws Exception {
    String cluster = "testViewCluster";
    ViewAggregatorMonitor monitor = new ViewAggregatorMonitor(cluster);
    monitor.register();
    ObjectName objectName = generateObjectName(cluster);

    monitor.recordProcessedSourceEvent();
    monitor.recordViewConfigProcessFailure();
    monitor.recordViewRefreshFailure();
    monitor.recordReadSourceFailure();
    monitor.recordRefreshViewLatency(100);

    Assert.assertEquals(
        (long) _beanServer.getAttribute(objectName, "ViewClusterRefreshFailureCounter"), 1);
    Assert.assertEquals(
        (long) _beanServer.getAttribute(objectName, "SourceClusterRefreshFailureCounter"), 1);
    Assert.assertEquals(
        (long) _beanServer.getAttribute(objectName, "ProcessedSourceClusterEventCounter"), 1);
    Assert.assertEquals(
        (long) _beanServer.getAttribute(objectName, "ProcessViewConfigFailureCounter"), 1);
    Assert.assertEquals(
        (long) _beanServer.getAttribute(objectName, "ViewClusterRefreshDurationGauge.Max"), 100);
    Assert
        .assertEquals(_beanServer.getAttribute(objectName, "ViewClusterRefreshDurationGauge.Mean"),
            100.0);
    Assert.assertEquals(
        _beanServer.getAttribute(objectName, "ViewClusterRefreshDurationGauge.StdDev"), 0.0);
  }

  private ObjectName generateObjectName(String viewClusterName) throws JMException {
    return MBeanRegistrar
        .buildObjectName(ViewAggregatorMonitor.MBEAN_DOMAIN, ViewAggregatorMonitor.MONITOR_KEY,
            viewClusterName);
  }

}
