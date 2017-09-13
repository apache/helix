package org.apache.helix.monitoring;

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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.TaskAssignmentStage;
import org.apache.helix.monitoring.mbeans.ClusterEventMonitor;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterEventStatusMonitor {

  @Test()
  public void test()
      throws InstanceNotFoundException, MalformedObjectNameException, NullPointerException,
      IOException, InterruptedException, MBeanException, AttributeNotFoundException,
      ReflectionException {
    System.out.println("START TestClusterEventStatusMonitor");
    String clusterName = "TestCluster";
    ClusterStatusMonitor monitor = new ClusterStatusMonitor(clusterName);

    MBeanServer _server = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectInstance> mbeans =
        _server.queryMBeans(new ObjectName("ClusterStatus:Cluster=TestCluster,eventName=ClusterEvent,*"), null);
    Assert.assertEquals(mbeans.size(), 0);

    int count = 5;
    Long totalDuration = 0L;
    for (int i = 1; i <= count; i++) {
      monitor.updateClusterEventDuration(ClusterEventMonitor.PhaseName.Callback.name(), 100 * i);
      monitor.updateClusterEventDuration(ClusterEventMonitor.PhaseName.InQueue.name(), 100 * i);
      monitor.updateClusterEventDuration(BestPossibleStateCalcStage.class.getSimpleName(), 100 * i);
      monitor.updateClusterEventDuration(ReadClusterDataStage.class.getSimpleName(), 100 * i);
      monitor.updateClusterEventDuration(IntermediateStateCalcStage.class.getSimpleName(), 100 * i);
      monitor.updateClusterEventDuration(TaskAssignmentStage.class.getSimpleName(), 100 * i);
      totalDuration += 100 * i;
    }

    mbeans =
        _server.queryMBeans(
            new ObjectName("ClusterStatus:cluster=TestCluster,eventName=ClusterEvent,*"), null);
    Assert.assertEquals(mbeans.size(), 6);

    for (ObjectInstance mbean : mbeans) {
      Long duration = (Long) _server.getAttribute(mbean.getObjectName(), "TotalDurationCounter");
      Long maxDuration = (Long) _server.getAttribute(mbean.getObjectName(), "MaxSingleDurationGauge");
      Long eventCount = (Long) _server.getAttribute(mbean.getObjectName(), "EventCounter");

      Assert.assertEquals(duration, totalDuration);
      Assert.assertEquals(maxDuration, Long.valueOf(100 * count));
      Assert.assertEquals(eventCount, Long.valueOf(count));
    }

    monitor.reset();

    mbeans =
        _server.queryMBeans(
            new ObjectName("ClusterStatus:cluster=TestCluster,eventName=ClusterEvent,*"), null);
    Assert.assertEquals(mbeans.size(), 0);

    System.out.println("END TestParticipantMonitor");
  }
}
