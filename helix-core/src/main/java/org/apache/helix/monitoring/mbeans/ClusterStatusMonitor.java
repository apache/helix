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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  static final String CLUSTER_STATUS_KEY = "ClusterStatus";
  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  static final String CLUSTER_DN_KEY = "cluster";
  static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private int _numOfLiveInstances = 0;
  private int _numOfInstances = 0;
  private int _numOfDisabledInstances = 0;
  private int _numOfDisabledPartitions = 0;

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap =
      new ConcurrentHashMap<String, ResourceMonitor>();

  private final ConcurrentHashMap<String, MessageQueueMonitor> _instanceMsgQueueMbeanMap =
      new ConcurrentHashMap<String, MessageQueueMonitor>();

  public ClusterStatusMonitor(String clusterName) {
    _clusterName = clusterName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      register(this, getObjectName(CLUSTER_DN_KEY + "=" + _clusterName));
    } catch (Exception e) {
      LOG.error("Register self failed.", e);
    }
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(CLUSTER_STATUS_KEY + ": " + name);
  }

  // Used by other external JMX consumers like ingraph
  public String getBeanName() {
    return CLUSTER_STATUS_KEY + " " + _clusterName;
  }

  @Override
  public long getDownInstanceGauge() {
    return _numOfInstances - _numOfLiveInstances;
  }

  @Override
  public long getInstancesGauge() {
    return _numOfInstances;
  }

  @Override
  public long getDisabledInstancesGauge() {
    return _numOfDisabledInstances;
  }

  @Override
  public long getDisabledPartitionsGauge() {
    return _numOfDisabledPartitions;
  }

  @Override
  public long getMaxMessageQueueSizeGauge() {
    long maxQueueSize = 0;
    for (MessageQueueMonitor msgQueue : _instanceMsgQueueMbeanMap.values()) {
      if (msgQueue.getMaxMessageQueueSize() > maxQueueSize) {
        maxQueueSize = (long) msgQueue.getMaxMessageQueueSize();
      }
    }

    return maxQueueSize;
  }

  @Override
  public String getMessageQueueSizes() {
    Map<String, Long> msgQueueSizes = new TreeMap<String, Long>();
    for (String instance : _instanceMsgQueueMbeanMap.keySet()) {
      MessageQueueMonitor msgQueue = _instanceMsgQueueMbeanMap.get(instance);
      msgQueueSizes.put(instance, new Long((long) msgQueue.getMaxMessageQueueSize()));
    }

    return msgQueueSizes.toString();
  }

  private void register(Object bean, ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      // OK
    }

    try {
      LOG.info("Registering " + name.toString());
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean" + name, e);
    }
  }

  private void unregister(ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        LOG.info("Unregistering " + name.toString());
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      LOG.warn("Could not unregister MBean" + name, e);
    }
  }

  public void setClusterStatusCounters(int numberLiveInstances, int numberOfInstances,
      int disabledInstances, int disabledPartitions) {
    _numOfInstances = numberOfInstances;
    _numOfLiveInstances = numberLiveInstances;
    _numOfDisabledInstances = disabledInstances;
    _numOfDisabledPartitions = disabledPartitions;
  }

  public void onExternalViewChange(ExternalView externalView, IdealState idealState) {
    try {
      String resourceName = externalView.getId();
      if (!_resourceMbeanMap.containsKey(resourceName)) {
        synchronized (this) {
          if (!_resourceMbeanMap.containsKey(resourceName)) {
            ResourceMonitor bean = new ResourceMonitor(_clusterName, resourceName);
            String beanName =
                CLUSTER_DN_KEY + "=" + _clusterName + "," + RESOURCE_DN_KEY + "=" + resourceName;
            register(bean, getObjectName(beanName));
            _resourceMbeanMap.put(resourceName, bean);
          }
        }
      }
      _resourceMbeanMap.get(resourceName).updateExternalView(externalView, idealState);
    } catch (Exception e) {
      LOG.warn(e);
    }
  }

  public void addMessageQueueSize(String instanceName, int msgQueueSize) {
    try {
      if (!_instanceMsgQueueMbeanMap.containsKey(instanceName)) {
        synchronized (this) {
          if (!_instanceMsgQueueMbeanMap.containsKey(instanceName)) {
            MessageQueueMonitor bean = new MessageQueueMonitor(_clusterName, instanceName);
            _instanceMsgQueueMbeanMap.put(instanceName, bean);
          }
        }
      }
      _instanceMsgQueueMbeanMap.get(instanceName).addMessageQueueSize(msgQueueSize);
    } catch (Exception e) {
      LOG.warn("fail to add message queue size to mbean", e);
    }
  }

  public void reset() {
    LOG.info("Resetting ClusterStatusMonitor");
    try {
      for (String resourceName : _resourceMbeanMap.keySet()) {
        String beanName =
            CLUSTER_DN_KEY + "=" + _clusterName + "," + RESOURCE_DN_KEY + "=" + resourceName;
        unregister(getObjectName(beanName));
      }
      _resourceMbeanMap.clear();

      for (MessageQueueMonitor bean : _instanceMsgQueueMbeanMap.values()) {
        bean.reset();
      }
      _instanceMsgQueueMbeanMap.clear();

      unregister(getObjectName(CLUSTER_DN_KEY + "=" + _clusterName));
    } catch (Exception e) {
      LOG.error("fail to reset ClusterStatusMonitor", e);
    }
  }

  @Override
  public String getSensorName() {
    return CLUSTER_STATUS_KEY + "_" + _clusterName;
  }

}
