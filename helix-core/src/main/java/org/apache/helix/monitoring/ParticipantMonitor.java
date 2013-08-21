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

import java.lang.management.ManagementFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.monitoring.mbeans.StateTransitionStatMonitor;
import org.apache.log4j.Logger;

public class ParticipantMonitor {
  private final ConcurrentHashMap<StateTransitionContext, StateTransitionStatMonitor> _monitorMap =
      new ConcurrentHashMap<StateTransitionContext, StateTransitionStatMonitor>();
  private static final Logger LOG = Logger.getLogger(ParticipantMonitor.class);

  private MBeanServer _beanServer;

  public ParticipantMonitor() {
    try {
      _beanServer = ManagementFactory.getPlatformMBeanServer();
    } catch (Exception e) {
      LOG.warn(e);
      e.printStackTrace();
      _beanServer = null;
    }
  }

  public void reportTransitionStat(StateTransitionContext cxt, StateTransitionDataPoint data) {
    if (_beanServer == null) {
      LOG.warn("bean server is null, skip reporting");
      return;
    }
    try {
      if (!_monitorMap.containsKey(cxt)) {
        synchronized (this) {
          if (!_monitorMap.containsKey(cxt)) {
            StateTransitionStatMonitor bean =
                new StateTransitionStatMonitor(cxt, TimeUnit.MILLISECONDS);
            _monitorMap.put(cxt, bean);
            String beanName = cxt.toString();
            register(bean, getObjectName(beanName));
          }
        }
      }
      _monitorMap.get(cxt).addDataPoint(data);
    } catch (Exception e) {
      LOG.warn(e);
      e.printStackTrace();
    }
  }

  private ObjectName getObjectName(String name) throws MalformedObjectNameException {
    LOG.info("Registering bean: " + name);
    return new ObjectName("CLMParticipantReport:" + name);
  }

  private void register(Object bean, ObjectName name) {
    if (_beanServer == null) {
      LOG.warn("bean server is null, skip reporting");
      return;
    }
    try {
      _beanServer.unregisterMBean(name);
    } catch (Exception e1) {
      // Swallow silently
    }

    try {
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean", e);
    }
  }

  public void shutDown() {
    for (StateTransitionContext cxt : _monitorMap.keySet()) {
      try {
        ObjectName name = getObjectName(cxt.toString());
        if (_beanServer.isRegistered(name)) {
          _beanServer.unregisterMBean(name);
        }
      } catch (Exception e) {
        LOG.warn("fail to unregister " + cxt.toString(), e);
      }
    }
    _monitorMap.clear();

  }
}
