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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  public static final String CLUSTER_STATUS_KEY = "ClusterStatus";
  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  static final String CLUSTER_DN_KEY = "cluster";
  static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";
  static final String MESSAGE_QUEUE_DN_KEY = "messageQueue";

  public static final String DEFAULT_TAG = "DEFAULT";

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private Set<String> _liveInstances = Collections.emptySet();
  private Set<String> _instances = Collections.emptySet();
  private Set<String> _disabledInstances = Collections.emptySet();
  private Map<String, Set<String>> _disabledPartitions = Collections.emptyMap();
  private Map<String, Long> _instanceMsgQueueSizes = Maps.newConcurrentMap();

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap =
      new ConcurrentHashMap<String, ResourceMonitor>();

  private final ConcurrentHashMap<String, InstanceMonitor> _instanceMbeanMap =
      new ConcurrentHashMap<String, InstanceMonitor>();

  /**
   * PerInstanceResource bean map: beanName->bean
   */
  private final Map<PerInstanceResourceMonitor.BeanName, PerInstanceResourceMonitor> _perInstanceResourceMap =
      new ConcurrentHashMap<PerInstanceResourceMonitor.BeanName, PerInstanceResourceMonitor>();

  public ClusterStatusMonitor(String clusterName) {
    _clusterName = clusterName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      register(this, getObjectName(clusterBeanName()));
    } catch (Exception e) {
      LOG.error("Fail to regiter ClusterStatusMonitor", e);
    }
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s: %s", CLUSTER_STATUS_KEY, name));
  }

  // TODO remove getBeanName()?
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
    for (Long queueSize : _instanceMsgQueueSizes.values()) {
      if (queueSize > maxQueueSize) {
        maxQueueSize = queueSize;
      }
    }
    return maxQueueSize;
  }

  @Override
  public long getInstanceMessageQueueBacklog() {
    long sum = 0;
    for (Long queueSize : _instanceMsgQueueSizes.values()) {
      sum += queueSize;
    }
    return sum;
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
      LOG.info("Register MBean: " + name);
      _beanServer.registerMBean(bean, name);
    } catch (Exception e) {
      LOG.warn("Could not register MBean: " + name, e);
    }
  }

  private void unregister(ObjectName name) {
    try {
      if (_beanServer.isRegistered(name)) {
        LOG.info("Unregistering " + name.toString());
        _beanServer.unregisterMBean(name);
      }
    } catch (Exception e) {
      LOG.warn("Could not unregister MBean: " + name, e);
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

  /**
   * Update gauges for resource at instance level
   * @param bestPossibleStates
   * @param resourceMap
   * @param stateModelDefMap
   */
  public void setPerInstanceResourceStatus(BestPossibleStateOutput bestPossibleStates,
      Map<String, InstanceConfig> instanceConfigMap, Map<ResourceId, ResourceConfig> resourceMap,
      Map<String, StateModelDefinition> stateModelDefMap) {

    // Convert to perInstanceResource beanName->partition->state
    Map<PerInstanceResourceMonitor.BeanName, Map<PartitionId, State>> beanMap =
        new HashMap<PerInstanceResourceMonitor.BeanName, Map<PartitionId, State>>();
    for (ResourceId resource : bestPossibleStates.getAssignedResources()) {
      ResourceAssignment assignment = bestPossibleStates.getResourceAssignment(resource);
      for (PartitionId partition : assignment.getMappedPartitionIds()) {
        Map<ParticipantId, State> instanceStateMap = assignment.getReplicaMap(partition);
        for (ParticipantId instance : instanceStateMap.keySet()) {
          State state = instanceStateMap.get(instance);
          PerInstanceResourceMonitor.BeanName beanName =
              new PerInstanceResourceMonitor.BeanName(instance.toString(), resource.toString());
          if (!beanMap.containsKey(beanName)) {
            beanMap.put(beanName, new HashMap<PartitionId, State>());
          }
          beanMap.get(beanName).put(partition, state);
        }
      }
    }
    // Unregister beans for per-instance resources that no longer exist
    Set<PerInstanceResourceMonitor.BeanName> toUnregister =
        Sets.newHashSet(_perInstanceResourceMap.keySet());
    toUnregister.removeAll(beanMap.keySet());
    try {
      unregisterPerInstanceResources(toUnregister);
    } catch (MalformedObjectNameException e) {
      LOG.error("Fail to unregister per-instance resource from MBean server: " + toUnregister, e);
    }
    // Register beans for per-instance resources that are newly configured
    Set<PerInstanceResourceMonitor.BeanName> toRegister = Sets.newHashSet(beanMap.keySet());
    toRegister.removeAll(_perInstanceResourceMap.keySet());
    Set<PerInstanceResourceMonitor> monitorsToRegister = Sets.newHashSet();
    for (PerInstanceResourceMonitor.BeanName beanName : toRegister) {
      PerInstanceResourceMonitor bean =
          new PerInstanceResourceMonitor(_clusterName, beanName.instanceName(),
              beanName.resourceName());
      StateModelDefId stateModelDefId =
          resourceMap.get(ResourceId.from(beanName.resourceName())).getIdealState()
              .getStateModelDefId();
      InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
      bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()),
          stateModelDefMap.get(stateModelDefId.toString()));
      monitorsToRegister.add(bean);
    }
    try {
      registerPerInstanceResources(monitorsToRegister);
    } catch (MalformedObjectNameException e) {
      LOG.error("Fail to register per-instance resource with MBean server: " + toRegister, e);
    }
    // Update existing beans
    for (PerInstanceResourceMonitor.BeanName beanName : _perInstanceResourceMap.keySet()) {
      PerInstanceResourceMonitor bean = _perInstanceResourceMap.get(beanName);
      StateModelDefId stateModelDefId =
          resourceMap.get(ResourceId.from(beanName.resourceName())).getIdealState()
              .getStateModelDefId();
      InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
      bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()),
          stateModelDefMap.get(stateModelDefId.toString()));
    }
  }

  /**
   * Indicate that a resource has been dropped, thus making it OK to drop its metrics
   * @param resourceName the resource that has been dropped
   */
  public void unregisterResource(String resourceName) {
    if (_resourceMbeanMap.containsKey(resourceName)) {
      synchronized (this) {
        if (_resourceMbeanMap.containsKey(resourceName)) {
          try {
            unregisterResources(Arrays.asList(resourceName));
          } catch (MalformedObjectNameException e) {
            LOG.error("Could not unregister beans for " + resourceName, e);
          }
        }
      }
    }
  }

  public void setResourceStatus(ExternalView externalView, IdealState idealState,
      StateModelDefinition stateModelDef) {
    String topState = null;
    if (stateModelDef != null) {
      List<String> priorityList = stateModelDef.getStatesPriorityList();
      if (!priorityList.isEmpty()) {
        topState = priorityList.get(0);
      }
    }
    try {
      String resourceName = externalView.getId();
      if (!_resourceMbeanMap.containsKey(resourceName)) {
        synchronized (this) {
          if (!_resourceMbeanMap.containsKey(resourceName)) {
            ResourceMonitor bean = new ResourceMonitor(_clusterName, resourceName);
            bean.updateResource(externalView, idealState, topState);
            registerResources(Arrays.asList(bean));
          }
        }
      }
      ResourceMonitor bean = _resourceMbeanMap.get(resourceName);
      String oldSensorName = bean.getSensorName();
      bean.updateResource(externalView, idealState, topState);
      String newSensorName = bean.getSensorName();
      if (!oldSensorName.equals(newSensorName)) {
        unregisterResources(Arrays.asList(resourceName));
        registerResources(Arrays.asList(bean));
      }
    } catch (Exception e) {
      LOG.error("Fail to set resource status, resource: " + idealState.getResourceName(), e);
    }
  }

  public void addMessageQueueSize(String instanceName, long msgQueueSize) {
    _instanceMsgQueueSizes.put(instanceName, msgQueueSize);
  }

  public void reset() {
    LOG.info("Reset ClusterStatusMonitor");
    try {
      unregisterResources(_resourceMbeanMap.keySet());

      _resourceMbeanMap.clear();

      _instanceMsgQueueSizes.clear();

      unregisterInstances(_instanceMbeanMap.keySet());
      _instanceMbeanMap.clear();

      unregisterPerInstanceResources(_perInstanceResourceMap.keySet());
      unregister(getObjectName(clusterBeanName()));
    } catch (Exception e) {
      LOG.error("Fail to reset ClusterStatusMonitor, cluster: " + _clusterName, e);
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

  private synchronized void registerPerInstanceResources(
      Collection<PerInstanceResourceMonitor> monitors) throws MalformedObjectNameException {
    for (PerInstanceResourceMonitor monitor : monitors) {
      String instanceName = monitor.getInstanceName();
      String resourceName = monitor.getResourceName();
      String beanName = getPerInstanceResourceBeanName(instanceName, resourceName);
      register(monitor, getObjectName(beanName));
      _perInstanceResourceMap.put(new PerInstanceResourceMonitor.BeanName(instanceName,
          resourceName), monitor);
    }
  }

  private synchronized void unregisterPerInstanceResources(
      Collection<PerInstanceResourceMonitor.BeanName> beanNames)
      throws MalformedObjectNameException {
    for (PerInstanceResourceMonitor.BeanName beanName : beanNames) {
      unregister(getObjectName(getPerInstanceResourceBeanName(beanName.instanceName(),
          beanName.resourceName())));
    }
    _perInstanceResourceMap.keySet().removeAll(beanNames);
  }

  public String clusterBeanName() {
    return String.format("%s=%s", CLUSTER_DN_KEY, _clusterName);
  }

  /**
   * Build instance bean name
   * @param instanceName
   * @return instance bean name
   */
  private String getInstanceBeanName(String instanceName) {
    return String.format("%s,%s=%s", clusterBeanName(), INSTANCE_DN_KEY, instanceName);
  }

  /**
   * Build resource bean name
   * @param resourceName
   * @return resource bean name
   */
  private String getResourceBeanName(String resourceName) {
    return String.format("%s,%s=%s", clusterBeanName(), RESOURCE_DN_KEY, resourceName);
  }

  /**
   * Build per-instance resource bean name:
   * "cluster={clusterName},instanceName={instanceName},resourceName={resourceName}"
   * @param instanceName
   * @param resourceName
   * @return per-instance resource bean name
   */
  public String getPerInstanceResourceBeanName(String instanceName, String resourceName) {
    return String.format("%s,%s", clusterBeanName(), new PerInstanceResourceMonitor.BeanName(
        instanceName, resourceName).toString());
  }

  @Override
  public String getSensorName() {
    return CLUSTER_STATUS_KEY + "." + _clusterName;
  }

}
