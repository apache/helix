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

import org.apache.helix.model.Message;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class ParticipantStatusMonitor {
  private final ConcurrentHashMap<StateTransitionContext, StateTransitionStatMonitor> _monitorMap =
      new ConcurrentHashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(ParticipantStatusMonitor.class);

  private MBeanServer _beanServer;
  private ParticipantMessageMonitor _messageMonitor;
  private MessageLatencyMonitor _messageLatencyMonitor;
  private Map<String, ThreadPoolExecutorMonitor> _executorMonitors;

  public ParticipantStatusMonitor(boolean isParticipant, String instanceName) {
    try {
      _beanServer = ManagementFactory.getPlatformMBeanServer();
      if (isParticipant) {
        _messageMonitor = new ParticipantMessageMonitor(instanceName);
        _messageLatencyMonitor =
            new MessageLatencyMonitor(MonitorDomainNames.CLMParticipantReport.name(), instanceName);
        _messageLatencyMonitor.register();
        _executorMonitors = new ConcurrentHashMap<>();
        register(_messageMonitor, getObjectName(_messageMonitor.getParticipantBeanName()));
      }
    } catch (Exception e) {
      LOG.warn(e.toString());
      e.printStackTrace();
      _beanServer = null;
    }
  }

  public synchronized void reportReceivedMessage(Message message) {
    if (_messageMonitor != null) {  // is participant
      _messageMonitor.incrementReceivedMessages(1);
      _messageMonitor.incrementPendingMessages(1);
      _messageLatencyMonitor.updateLatency(message);
    }
  }

  public synchronized void reportProcessedMessage(Message message,
      ParticipantMessageMonitor.ProcessedMessageState processedMessageState) {
    if (_messageMonitor != null) {  // is participant
      switch (processedMessageState) {
        case DISCARDED:
          _messageMonitor.incrementDiscardedMessages(1);
          _messageMonitor.decrementPendingMessages(1);
          break;
        case FAILED:
          _messageMonitor.incrementFailedMessages(1);
          _messageMonitor.decrementPendingMessages(1);
          break;
        case COMPLETED:
          _messageMonitor.incrementCompletedMessages(1);
          _messageMonitor.decrementPendingMessages(1);
          break;
      }
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
                new StateTransitionStatMonitor(cxt, getObjectName(cxt.toString()));
            _monitorMap.put(cxt, bean);
            bean.register();
          }
        }
      }
      _monitorMap.get(cxt).addDataPoint(data);
    } catch (Exception e) {
      LOG.warn(e.toString());
      e.printStackTrace();
    }
  }

  private ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s", MonitorDomainNames.CLMParticipantReport.name(), name));
  }

  private void register(Object bean, ObjectName name) {
    LOG.info("Registering bean: " + name.toString());
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
    if (_messageMonitor != null) {  // is participant
      try {
        ObjectName name = getObjectName(_messageMonitor.getParticipantBeanName());
        if (_beanServer.isRegistered(name)) {
          _beanServer.unregisterMBean(name);
        }
      } catch (Exception e) {
        LOG.warn("fail to unregister " + _messageMonitor.getParticipantBeanName(), e);
      }
    }
    if (_messageLatencyMonitor != null) {
      _messageLatencyMonitor.unregister();
    }
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

  public void createExecutorMonitor(String type, ExecutorService executor) {
    if (_executorMonitors == null) {
      return;
    }
    if (! (executor instanceof ThreadPoolExecutor)) {
      return;
    }

    try {
      _executorMonitors.put(type,
          new ThreadPoolExecutorMonitor(type, (ThreadPoolExecutor) executor));
    } catch (JMException e) {
      LOG.warn(String.format(
          "Error in creating ThreadPoolExecutorMonitor for type=%s", type), e);
    }
  }

  public void removeExecutorMonitor(String type) {
    if (_executorMonitors != null) {
      ThreadPoolExecutorMonitor monitor = _executorMonitors.remove(type);
      if (monitor != null) {
        monitor.unregister();
      }
    }
  }
}
