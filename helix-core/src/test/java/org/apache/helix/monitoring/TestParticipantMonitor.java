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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerNotification;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.monitoring.mbeans.ClusterMBeanObserver;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.ParticipantStatusMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestParticipantMonitor {
  private static Logger _logger = LoggerFactory.getLogger(TestParticipantMonitor.class);
  private static String CLUSTER_NAME = TestHelper.getTestClassName();

  class ParticipantMonitorListener extends ClusterMBeanObserver {
    Map<String, Map<String, Object>> _beanValueMap = new HashMap<>();

    public ParticipantMonitorListener(String domain) throws IOException, InstanceNotFoundException {
      super(domain);
      init();
    }

    void init() {
      try {
        Set<ObjectInstance> existingInstances =
            _server.queryMBeans(new ObjectName(_domain + ":Cluster=" + CLUSTER_NAME + ",*"), null);
        for (ObjectInstance instance : existingInstances) {
          String mbeanName = instance.getObjectName().toString();
          // System.out.println("mbeanName: " + mbeanName);
          addMBean(instance.getObjectName());
        }
      } catch (Exception e) {
        _logger.warn("fail to get all existing mbeans in " + _domain, e);
      }
    }

    @Override
    public void onMBeanRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {
      addMBean(mbsNotification.getMBeanName());
    }

    void addMBean(ObjectName beanName) {
      try {
        MBeanInfo info = _server.getMBeanInfo(beanName);
        MBeanAttributeInfo[] infos = info.getAttributes();
        _beanValueMap.put(beanName.toString(), new HashMap<String, Object>());
        for (MBeanAttributeInfo infoItem : infos) {
          Object val = _server.getAttribute(beanName, infoItem.getName());
          // System.out.println("         " + infoItem.getName() + " : " +
          // _server.getAttribute(beanName, infoItem.getName()) + " type : " + infoItem.getType());
          _beanValueMap.get(beanName.toString()).put(infoItem.getName(), val);
        }
      } catch (Exception e) {
        _logger.error("Error getting bean info, domain=" + _domain, e);
      }
    }

    @Override
    public void onMBeanUnRegistered(MBeanServerConnection server,
        MBeanServerNotification mbsNotification) {

    }
  }

  private ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(
        String.format("%s:%s", MonitorDomainNames.CLMParticipantReport.name(), name));
  }

  @Test()
  public void testReportData()
      throws InstanceNotFoundException, MalformedObjectNameException, NullPointerException,
      IOException, InterruptedException {
    System.out.println("START TestParticipantMonitor");
    ParticipantStatusMonitor monitor = new ParticipantStatusMonitor(false, null);

    int monitorNum = 0;

    StateTransitionContext cxt = new StateTransitionContext(CLUSTER_NAME, "instance", "db_1", "a-b");
    StateTransitionDataPoint data = new StateTransitionDataPoint(2000, 1000, 600, true);
    monitor.reportTransitionStat(cxt, data);

    data = new StateTransitionDataPoint(2000, 1200, 600, true);
    monitor.reportTransitionStat(cxt, data);

    ParticipantMonitorListener monitorListener =
        new ParticipantMonitorListener("CLMParticipantReport");
    Thread.sleep(1000);
    Assert.assertEquals(monitorListener._beanValueMap.size(), monitorNum + 1);

    // Note the values in listener's map is the snapshot when the MBean is detected.
    Assert.assertEquals(monitorListener._beanValueMap.get(getObjectName(cxt.toString()).toString())
        .get("MeanTransitionLatency"), 2000.0);
    Assert.assertEquals(monitorListener._beanValueMap.get(getObjectName(cxt.toString()).toString())
        .get("MeanTransitionExecuteLatency"), 1100.0);
    Assert.assertEquals(monitorListener._beanValueMap.get(getObjectName(cxt.toString()).toString())
        .get("MeanTransitionMessageLatency"), 600.0);
    Assert.assertEquals(monitorListener._beanValueMap.get(getObjectName(cxt.toString()).toString())
        .get("TotalStateTransitionGauge"), 2L);

    data = new StateTransitionDataPoint(2000, 500, 600, true);
    monitor.reportTransitionStat(cxt, data);
    Thread.sleep(1000);
    Assert.assertEquals(monitorListener._beanValueMap.size(), monitorNum + 1);

    data = new StateTransitionDataPoint(1000, 500, 300, true);
    StateTransitionContext cxt2 = new StateTransitionContext(CLUSTER_NAME, "instance", "db_2", "a-b");
    monitor.reportTransitionStat(cxt2, data);
    monitor.reportTransitionStat(cxt2, data);
    Thread.sleep(1000);
    Assert.assertEquals(monitorListener._beanValueMap.size(), monitorNum + 2);

    Assert.assertFalse(cxt.equals(cxt2));
    Assert.assertFalse(cxt.equals(new Object()));
    Assert.assertTrue(cxt.equals(new StateTransitionContext(CLUSTER_NAME, "instance", "db_1", "a-b")));

    cxt2.getInstanceName();

    ParticipantMonitorListener monitorListener2 =
        new ParticipantMonitorListener("CLMParticipantReport");

    Thread.sleep(1000);
    Assert.assertEquals(monitorListener2._beanValueMap.size(), monitorNum + 2);

    monitorListener2.disconnect();
    monitorListener.disconnect();
    System.out.println("END TestParticipantMonitor");
  }
}
