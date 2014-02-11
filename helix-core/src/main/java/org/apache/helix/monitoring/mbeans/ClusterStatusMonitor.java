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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  static final String CLUSTER_STATUS_KEY = "ClusterStatus";
  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  static final String CLUSTER_DN_KEY = "cluster";
  static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";

  static final String DEFAULT_TAG = "DEFAULT";

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private Set<String> _liveInstances = Collections.emptySet();
  private Set<String> _instances = Collections.emptySet();
  private Set<String> _disabledInstances = Collections.emptySet();
  private Map<String, Set<String>> _disabledPartitions = Collections.emptyMap();

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap =
      new ConcurrentHashMap<String, ResourceMonitor>();

  private final ConcurrentHashMap<String, MessageQueueMonitor> _instanceMsgQueueMbeanMap =
      new ConcurrentHashMap<String, MessageQueueMonitor>();

  private final ConcurrentHashMap<String, InstanceMonitor> _instanceMbeanMap =
      new ConcurrentHashMap<String, InstanceMonitor>();

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
    return _instances.size() - _liveInstances.size();
  }

  @Override
  public long getInstancesGauge() {
    return _instances.size();
  }

  @Override
  public long getDisabledInstancesGauge() {
    return _disabledInstances.size();
  }

  @Override
  public long getDisabledPartitionsGauge() {
    int numDisabled = 0;
    for (String instance : _disabledPartitions.keySet()) {
      numDisabled += _disabledPartitions.get(instance).size();
    }
    return numDisabled;
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

  /**
   * Update the gauges for all instances in the cluster
   * @param liveInstanceSet the current set of live instances
   * @param instanceSet the current set of configured instances (live or other
   * @param disabledInstanceSet the current set of configured instances that are disabled
   * @param disabledPartitions a map of instance name to the set of partitions disabled on it
   * @param tags a map of instance name to the set of tags on it
   */
  public void setClusterInstanceStatus(Set<String> liveInstanceSet, Set<String> instanceSet,
      Set<String> disabledInstanceSet, Map<String, Set<String>> disabledPartitions,
      Map<String, Set<String>> tags) {
    // Unregister beans for instances that are no longer configured
    Set<String> toUnregister = Sets.newHashSet(_instanceMbeanMap.keySet());
    toUnregister.removeAll(instanceSet);
    try {
      unregisterInstances(toUnregister);
    } catch (MalformedObjectNameException e) {
      LOG.error("Could not unregister instances from MBean server: " + toUnregister, e);
    }

    // Register beans for instances that are newly configured
    Set<String> toRegister = Sets.newHashSet(instanceSet);
    toRegister.removeAll(_instanceMbeanMap.keySet());
    Set<InstanceMonitor> monitorsToRegister = Sets.newHashSet();
    for (String instanceName : toRegister) {
      InstanceMonitor bean = new InstanceMonitor(_clusterName, instanceName);
      bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
          liveInstanceSet.contains(instanceName), !disabledInstanceSet.contains(instanceName));
      monitorsToRegister.add(bean);
    }
    try {
      registerInstances(monitorsToRegister);
    } catch (MalformedObjectNameException e) {
      LOG.error("Could not register instances with MBean server: " + toRegister, e);
    }

    // Update all the sets
    _instances = instanceSet;
    _liveInstances = liveInstanceSet;
    _disabledInstances = disabledInstanceSet;
    _disabledPartitions = disabledPartitions;

    // Update the instance MBeans
    for (String instanceName : instanceSet) {
      if (_instanceMbeanMap.containsKey(instanceName)) {
        // Update the bean
        InstanceMonitor bean = _instanceMbeanMap.get(instanceName);
        String oldSensorName = bean.getSensorName();
        bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
            liveInstanceSet.contains(instanceName), !disabledInstanceSet.contains(instanceName));

        // If the sensor name changed, re-register the bean so that listeners won't miss it
        String newSensorName = bean.getSensorName();
        if (!oldSensorName.equals(newSensorName)) {
          try {
            unregisterInstances(Arrays.asList(instanceName));
            registerInstances(Arrays.asList(bean));
          } catch (MalformedObjectNameException e) {
            LOG.error("Could not refresh registration with MBean server: " + instanceName, e);
          }
        }
      }
    }
  }

  public void onExternalViewChange(ExternalView externalView, IdealState idealState) {
    try {
      String resourceName = externalView.getId();
      if (!_resourceMbeanMap.containsKey(resourceName)) {
        synchronized (this) {
          if (!_resourceMbeanMap.containsKey(resourceName)) {
            ResourceMonitor bean = new ResourceMonitor(_clusterName, resourceName);
            bean.updateExternalView(externalView, idealState);
            registerResources(Arrays.asList(bean));
          }
        }
      }
      ResourceMonitor bean = _resourceMbeanMap.get(resourceName);
      String oldSensorName = bean.getSensorName();
      bean.updateExternalView(externalView, idealState);
      String newSensorName = bean.getSensorName();
      if (!oldSensorName.equals(newSensorName)) {
        unregisterResources(Arrays.asList(resourceName));
        registerResources(Arrays.asList(bean));
      }
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

      unregisterInstances(_instanceMbeanMap.keySet());
      _instanceMbeanMap.clear();

      unregister(getObjectName(CLUSTER_DN_KEY + "=" + _clusterName));
    } catch (Exception e) {
      LOG.error("fail to reset ClusterStatusMonitor", e);
    }
  }

  private synchronized void registerInstances(Collection<InstanceMonitor> instances)
      throws MalformedObjectNameException {
    for (InstanceMonitor monitor : instances) {
      String instanceName = monitor.getInstanceName();
      String beanName = getInstanceBeanName(instanceName);
      register(monitor, getObjectName(beanName));
      _instanceMbeanMap.put(instanceName, monitor);
    }
  }

  private synchronized void unregisterInstances(Collection<String> instances)
      throws MalformedObjectNameException {
    for (String instanceName : instances) {
      String beanName = getInstanceBeanName(instanceName);
      unregister(getObjectName(beanName));
    }
    _instanceMbeanMap.keySet().removeAll(instances);
  }

  private synchronized void registerResources(Collection<ResourceMonitor> resources)
      throws MalformedObjectNameException {
    for (ResourceMonitor monitor : resources) {
      String resourceName = monitor.getResourceName();
      String beanName = getResourceBeanName(resourceName);
      register(monitor, getObjectName(beanName));
      _resourceMbeanMap.put(resourceName, monitor);
    }
  }

  private synchronized void unregisterResources(Collection<String> resources)
      throws MalformedObjectNameException {
    for (String resourceName : resources) {
      String beanName = getResourceBeanName(resourceName);
      unregister(getObjectName(beanName));
    }
    _resourceMbeanMap.keySet().removeAll(resources);
  }

  private String getInstanceBeanName(String instanceName) {
    return CLUSTER_DN_KEY + "=" + _clusterName + "," + INSTANCE_DN_KEY + "=" + instanceName;
  }

  private String getResourceBeanName(String resourceName) {
    return CLUSTER_DN_KEY + "=" + _clusterName + "," + RESOURCE_DN_KEY + "=" + resourceName;
  }

  @Override
  public String getSensorName() {
    return CLUSTER_STATUS_KEY + "." + _clusterName;
  }

}
