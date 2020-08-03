package org.apache.helix.controller.rebalancer.constraint;

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
import java.util.Random;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.metrics.AbnormalStatesMetricCollector;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAbnormalStatesResolverMonitor {
  private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
  private final String CLUSTER_NAME = "TestCluster";

  @Test
  public void testMonitorResolver()
      throws MalformedObjectNameException, AttributeNotFoundException, MBeanException,
      ReflectionException, InstanceNotFoundException {
    final String testResolverMonitorMbeanName = String
        .format("%s:%s=%s, %s=%s.%s", MonitorDomainNames.Rebalancer, "ClusterName", CLUSTER_NAME,
            "EntityName", "AbnormalStates", MasterSlaveSMD.name);
    final ObjectName testResolverMonitorMbeanObjectName =
        new ObjectName(testResolverMonitorMbeanName);

    Assert.assertFalse(MBEAN_SERVER.isRegistered(testResolverMonitorMbeanObjectName));

    // Update the resolver configuration for MasterSlave state model.
    MonitoredAbnormalResolver monitoredAbnormalResolver =
        new MonitoredAbnormalResolver(new MockAbnormalStateResolver(), CLUSTER_NAME,
            MasterSlaveSMD.name);

    // Validate if the MBean has been registered
    Assert.assertTrue(MBEAN_SERVER.isRegistered(testResolverMonitorMbeanObjectName));
    Assert.assertEquals(MBEAN_SERVER.getAttribute(testResolverMonitorMbeanObjectName,
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.AbnormalStatePartitionCounter
            .name()), 0L);
    Assert.assertEquals(MBEAN_SERVER.getAttribute(testResolverMonitorMbeanObjectName,
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.RecoveryAttemptCounter.name()), 0L);
    // Validate if the metrics recording methods work as expected
    Random ran = new Random(System.currentTimeMillis());
    Long expectation = 1L + ran.nextInt(10);
    for (int i = 0; i < expectation; i++) {
      monitoredAbnormalResolver.recordAbnormalState();
    }
    Assert.assertEquals(MBEAN_SERVER.getAttribute(testResolverMonitorMbeanObjectName,
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.AbnormalStatePartitionCounter
            .name()), expectation);
    expectation = 1L + ran.nextInt(10);
    for (int i = 0; i < expectation; i++) {
      monitoredAbnormalResolver.recordRecoveryAttempt();
    }
    Assert.assertEquals(MBEAN_SERVER.getAttribute(testResolverMonitorMbeanObjectName,
        AbnormalStatesMetricCollector.AbnormalStatesMetricNames.RecoveryAttemptCounter.name()),
        expectation);

    // Reset the resolver map
    monitoredAbnormalResolver.close();
    // Validate if the MBean has been unregistered
    Assert.assertFalse(MBEAN_SERVER.isRegistered(testResolverMonitorMbeanObjectName));
  }
}
