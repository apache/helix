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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.JMException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestInstanceMonitor {
  @Test
  public void testInstanceMonitor()
      throws JMException {
    String testCluster = "testCluster";
    String testInstance = "testInstance";
    String testDomain = "testDomain:key=value";
    Set<String> tags = ImmutableSet.of("test", "DEFAULT");
    Map<String, List<String>> disabledPartitions =
        ImmutableMap.of("instance1", ImmutableList.of("partition1", "partition2"));
    InstanceMonitor monitor =
        new InstanceMonitor(testCluster, testInstance, new ObjectName(testDomain));

    // Verify init status.
    Assert.assertEquals(monitor.getSensorName(),
        "ParticipantStatus.testCluster.DEFAULT.testInstance");
    Assert.assertEquals(monitor.getInstanceName(), testInstance);
    Assert.assertEquals(monitor.getOnline(), 0L);
    Assert.assertEquals(monitor.getEnabled(), 0L);
    Assert.assertEquals(monitor.getTotalMessageReceived(), 0L);
    Assert.assertEquals(monitor.getDisabledPartitions(), 0L);
    Assert.assertEquals(monitor.getMaxCapacityUsageGauge(), 0.0d);

    // Update metrics.
    monitor.updateMaxCapacityUsage(0.5d);
    monitor.increaseMessageCount(10L);
    monitor.updateInstance(tags, disabledPartitions, Collections.emptyList(), true, true);

    // Verify metrics.
    Assert.assertEquals(monitor.getMaxCapacityUsageGauge(), 0.5d);
    Assert.assertEquals(monitor.getTotalMessageReceived(), 10L);
    Assert.assertEquals(monitor.getSensorName(),
        "ParticipantStatus.testCluster.DEFAULT|test.testInstance");
    Assert.assertEquals(monitor.getInstanceName(), testInstance);
    Assert.assertEquals(monitor.getOnline(), 1L);
    Assert.assertEquals(monitor.getEnabled(), 1L);
    Assert.assertEquals(monitor.getDisabledPartitions(), 2L);

    monitor.unregister();
  }
}
