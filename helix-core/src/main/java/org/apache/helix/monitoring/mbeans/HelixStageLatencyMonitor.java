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

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.helix.monitoring.StatCollector;
import org.apache.log4j.Logger;

public class HelixStageLatencyMonitor implements HelixStageLatencyMonitorMBean {
  private static final Logger LOG = Logger.getLogger(HelixStageLatencyMonitor.class);

  private final StatCollector _stgLatency;
  private final MBeanServer _beanServer;
  private final String _clusterName;
  private final String _stageName;
  private final ObjectName _objectName;

  public HelixStageLatencyMonitor(String clusterName, String stageName) throws Exception {
    _clusterName = clusterName;
    _stageName = stageName;
    _stgLatency = new StatCollector();
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    _objectName =
        new ObjectName("StageLatencyMonitor: " + "cluster=" + _clusterName + ",stage=" + _stageName);
    try {
      register(this, _objectName);
    } catch (Exception e) {
      LOG.error("Couldn't register " + _objectName + " mbean", e);
      throw e;
    }
  }

  private void register(Object bean, ObjectName name) throws Exception {
    try {
      _beanServer.unregisterMBean(name);
    } catch (Exception e) {
      // OK
    }

    _beanServer.registerMBean(bean, name);
  }

  private void unregister(ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      LOG.error("Couldn't unregister " + _objectName + " mbean", e);
    }
  }

  public void addStgLatency(long time) {
    _stgLatency.addData(time);
  }

  public void reset() {
    _stgLatency.reset();
    unregister(_objectName);
  }

  @Override
  public long getMaxStgLatency() {
    return (long) _stgLatency.getMax();
  }

  @Override
  public long getMeanStgLatency() {
    return (long) _stgLatency.getMean();
  }

  @Override
  public long get95StgLatency() {
    return (long) _stgLatency.getPercentile(95);
  }

}
