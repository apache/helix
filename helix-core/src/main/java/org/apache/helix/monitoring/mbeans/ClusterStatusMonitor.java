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

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.controller.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterStatusMonitor.class);

  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  public static final String CLUSTER_DN_KEY = "cluster";
  public static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";
  static final String MESSAGE_QUEUE_DN_KEY = "messageQueue";
  static final String WORKFLOW_TYPE_DN_KEY = "workflowType";
  static final String JOB_TYPE_DN_KEY = "jobType";
  static final String DEFAULT_WORKFLOW_JOB_TYPE = "DEFAULT";
  public static final String DEFAULT_TAG = "DEFAULT";

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private boolean _enabled = true;
  private boolean _inMaintenance = false;
  private boolean _paused = false;

  private Set<String> _liveInstances = Collections.emptySet();
  private Set<String> _instances = Collections.emptySet();
  private Set<String> _disabledInstances = Collections.emptySet();
  private Map<String, Map<String, List<String>>> _disabledPartitions = Collections.emptyMap();
  private Map<String, List<String>> _oldDisabledPartitions = Collections.emptyMap();
  private Map<String, Long> _instanceMsgQueueSizes = Maps.newConcurrentMap();
  private boolean _rebalanceFailure = false;
  private AtomicLong _rebalanceFailureCount = new AtomicLong(0L);

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMonitorMap =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, InstanceMonitor> _instanceMonitorMap =
      new ConcurrentHashMap<>();

  // phaseName -> eventMonitor
  protected final ConcurrentHashMap<String, ClusterEventMonitor> _clusterEventMonitorMap =
      new ConcurrentHashMap<>();

  /**
   * PerInstanceResource monitor map: beanName->monitor
   */
  private final Map<PerInstanceResourceMonitor.BeanName, PerInstanceResourceMonitor>
      _perInstanceResourceMonitorMap = new ConcurrentHashMap<>();

  private final Map<String, WorkflowMonitor> _perTypeWorkflowMonitorMap = new ConcurrentHashMap<>();

  private final Map<String, JobMonitor> _perTypeJobMonitorMap = new ConcurrentHashMap<>();

  public ClusterStatusMonitor(String clusterName) {
    _clusterName = clusterName;
    _beanServer = ManagementFactory.getPlatformMBeanServer();
  }

  public ObjectName getObjectName(String name) throws MalformedObjectNameException {
    return new ObjectName(String.format("%s:%s", MonitorDomainNames.ClusterStatus.name(), name));
  }

  public String getClusterName() {
    return _clusterName;
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
    for (Map<String, List<String>> perInstance : _disabledPartitions.values()) {
      for (List<String> partitions : perInstance.values()) {
        if (partitions != null) {
          numDisabled += partitions.size();
        }
      }
    }

    // TODO : Get rid of this after old API removed.
    for (List<String> partitions : _oldDisabledPartitions.values()) {
      if (partitions != null) {
        numDisabled += partitions.size();
      }
    }

    return numDisabled;
  }

  @Override
  public long getRebalanceFailureGauge() {
    return _rebalanceFailure ? 1 : 0;
  }

  public void setRebalanceFailureGauge(boolean isFailure) {
    this._rebalanceFailure = isFailure;
  }

  public void setResourceRebalanceStates(Collection<String> resources,
      ResourceMonitor.RebalanceStatus state) {
    for (String resource : resources) {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resource);
      if (resourceMonitor != null) {
        resourceMonitor.setRebalanceState(state);
      }
    }
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
      Set<String> disabledInstanceSet, Map<String, Map<String, List<String>>> disabledPartitions,
      Map<String, List<String>> oldDisabledPartitions, Map<String, Set<String>> tags) {
    synchronized (_instanceMonitorMap) {
      // Unregister beans for instances that are no longer configured
      Set<String> toUnregister = Sets.newHashSet(_instanceMonitorMap.keySet());
      toUnregister.removeAll(instanceSet);
      try {
        unregisterInstances(toUnregister);
      } catch (MalformedObjectNameException e) {
        LOG.error("Could not unregister instances from MBean server: " + toUnregister, e);
      }

      // Register beans for instances that are newly configured
      Set<String> toRegister = Sets.newHashSet(instanceSet);
      toRegister.removeAll(_instanceMonitorMap.keySet());
      Set<InstanceMonitor> monitorsToRegister = Sets.newHashSet();
      for (String instanceName : toRegister) {
        InstanceMonitor bean = new InstanceMonitor(_clusterName, instanceName);
        bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
            oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName), !disabledInstanceSet.contains(instanceName));
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
      _oldDisabledPartitions = oldDisabledPartitions;

      // Update the instance MBeans
      for (String instanceName : instanceSet) {
        if (_instanceMonitorMap.containsKey(instanceName)) {
          // Update the bean
          InstanceMonitor bean = _instanceMonitorMap.get(instanceName);
          String oldSensorName = bean.getSensorName();
          bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
              oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName), !disabledInstanceSet.contains(instanceName));

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
  }

  /**
   * Update the duration of handling a cluster event in a certain phase.
   *
   * @param phase
   * @param duration
   */
  public void updateClusterEventDuration(String phase, long duration) {
    ClusterEventMonitor monitor = getOrCreateClusterEventMonitor(phase);
    if (monitor != null) {
      monitor.reportDuration(duration);
    }
  }

  private ClusterEventMonitor getOrCreateClusterEventMonitor(String phase) {
    try {
      if (!_clusterEventMonitorMap.containsKey(phase)) {
        synchronized (_clusterEventMonitorMap) {
          if (!_clusterEventMonitorMap.containsKey(phase)) {
            ClusterEventMonitor monitor = new ClusterEventMonitor(this, phase);
            monitor.register();
            _clusterEventMonitorMap.put(phase, monitor);
          }
        }
      }
    } catch (JMException e) {
      LOG.error("Failed to register ClusterEventMonitorMbean for cluster " + _clusterName
          + " and phase type: " + phase, e);
    }

    return _clusterEventMonitorMap.get(phase);
  }

  /**
   * Update message count per instance and per resource
   * @param messages a list of messages
   */
  public void increaseMessageReceived(List<Message> messages) {
    Map<String, Long> messageCountPerInstance = new HashMap<>();
    Map<String, Long> messageCountPerResource = new HashMap<>();

    // Aggregate messages
    for (Message message : messages) {
      String instanceName = message.getAttribute(Message.Attributes.TGT_NAME);
      String resourceName = message.getAttribute(Message.Attributes.RESOURCE_NAME);

      if (instanceName != null) {
        if (!messageCountPerInstance.containsKey(instanceName)) {
          messageCountPerInstance.put(instanceName, 0L);
        }
        messageCountPerInstance.put(instanceName, messageCountPerInstance.get(instanceName) + 1L);
      }

      if (resourceName != null) {
        if (!messageCountPerResource.containsKey(resourceName)) {
          messageCountPerResource.put(resourceName, 0L);
        }
        messageCountPerResource.put(resourceName, messageCountPerResource.get(resourceName) + 1L);
      }
    }

    // Update message count per instance and per resource
    for (String instance : messageCountPerInstance.keySet()) {
      InstanceMonitor instanceMonitor = _instanceMonitorMap.get(instance);
      if (instanceMonitor != null) {
        instanceMonitor.increaseMessageCount(messageCountPerInstance.get(instance));
      }
    }
    for (String resource : messageCountPerResource.keySet()) {
      ResourceMonitor resourceMonitor = _resourceMonitorMap.get(resource);
      if (resourceMonitor != null) {
        resourceMonitor.increaseMessageCount(messageCountPerResource.get(resource));
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
      Map<String, InstanceConfig> instanceConfigMap, Map<String, Resource> resourceMap,
      Map<String, StateModelDefinition> stateModelDefMap) {

    // Convert to perInstanceResource beanName->partition->state
    Map<PerInstanceResourceMonitor.BeanName, Map<Partition, String>> beanMap =
        new HashMap<>();
    Set<String> resourceSet = new HashSet<>(bestPossibleStates.resourceSet());
    for (String resource : resourceSet) {
      Map<Partition, Map<String, String>> partitionStateMap =
          new HashMap<>(bestPossibleStates.getResourceMap(resource));
      for (Partition partition : partitionStateMap.keySet()) {
        Map<String, String> instanceStateMap = partitionStateMap.get(partition);
        for (String instance : instanceStateMap.keySet()) {
          String state = instanceStateMap.get(instance);
          PerInstanceResourceMonitor.BeanName beanName = new PerInstanceResourceMonitor.BeanName(instance, resource);
          if (!beanMap.containsKey(beanName)) {
            beanMap.put(beanName, new HashMap<Partition, String>());
          }
          beanMap.get(beanName).put(partition, state);
        }
      }
    }
    synchronized (_perInstanceResourceMonitorMap) {
      // Unregister beans for per-instance resources that no longer exist
      Set<PerInstanceResourceMonitor.BeanName> toUnregister = Sets.newHashSet(
          _perInstanceResourceMonitorMap.keySet());
      toUnregister.removeAll(beanMap.keySet());
      try {
        unregisterPerInstanceResources(toUnregister);
      } catch (MalformedObjectNameException e) {
        LOG.error("Fail to unregister per-instance resource from MBean server: " + toUnregister, e);
      }
      // Register beans for per-instance resources that are newly configured
      Set<PerInstanceResourceMonitor.BeanName> toRegister = Sets.newHashSet(beanMap.keySet());
      toRegister.removeAll(_perInstanceResourceMonitorMap.keySet());
      Set<PerInstanceResourceMonitor> monitorsToRegister = Sets.newHashSet();
      for (PerInstanceResourceMonitor.BeanName beanName : toRegister) {
        PerInstanceResourceMonitor bean =
            new PerInstanceResourceMonitor(_clusterName, beanName.instanceName(), beanName.resourceName());
        String stateModelDefName = resourceMap.get(beanName.resourceName()).getStateModelDefRef();
        InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
        bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()), stateModelDefMap.get(stateModelDefName));
        monitorsToRegister.add(bean);
      }
      try {
        registerPerInstanceResources(monitorsToRegister);
      } catch (MalformedObjectNameException e) {
        LOG.error("Fail to register per-instance resource with MBean server: " + toRegister, e);
      }
      // Update existing beans
      for (PerInstanceResourceMonitor.BeanName beanName : _perInstanceResourceMonitorMap.keySet()) {
        PerInstanceResourceMonitor bean = _perInstanceResourceMonitorMap.get(beanName);
        String stateModelDefName = resourceMap.get(beanName.resourceName()).getStateModelDefRef();
        InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
        bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()), stateModelDefMap.get(stateModelDefName));
      }
    }
  }

  /**
   * Cleanup resource monitors. Keep the monitors if only exist in the input set.
   * @param resourceNames the resources that still exist
   */
  public void retainResourceMonitor(Set<String> resourceNames) {
    Set<String> resourcesToRemove = new HashSet<>();
    synchronized (_resourceMonitorMap) {
      resourceNames.retainAll(_resourceMonitorMap.keySet());
      resourcesToRemove.addAll(_resourceMonitorMap.keySet());
    }
    resourcesToRemove.removeAll(resourceNames);

    try {
      registerResources(resourceNames);
    } catch (JMException e) {
      LOG.error(String.format("Could not register beans for the following resources: %s",
          Joiner.on(',').join(resourceNames)), e);
    }

    try {
      unregisterResources(resourcesToRemove);
    } catch (Exception e) {
      LOG.error(String.format("Could not unregister beans for the following resources: %s",
          Joiner.on(',').join(resourcesToRemove)), e);
    }
  }

  public void setResourceStatus(ExternalView externalView, IdealState idealState,
      StateModelDefinition stateModelDef, int messageCount) {
    try {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(externalView.getId());

      if (resourceMonitor != null) {
        resourceMonitor.updateResourceState(externalView, idealState, stateModelDef);
        resourceMonitor.updatePendingStateTransitionMessages(messageCount);
      }
    } catch (Exception e) {
      LOG.error("Fail to set resource status, resource: " + idealState.getResourceName(), e);
    }
  }

  public void updateMissingTopStateDurationStats(String resourceName,
      long totalDuration, long helixLatency, boolean isGraceful, boolean succeeded) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateStateHandoffStats(ResourceMonitor.MonitorState.TOP_STATE, totalDuration,
          helixLatency, isGraceful, succeeded);
    }
  }

  public void updateRebalancerStats(String resourceName, long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateRebalancerStats(numPendingRecoveryRebalancePartitions,
          numPendingLoadRebalancePartitions, numRecoveryRebalanceThrottledPartitions,
          numLoadRebalanceThrottledPartitions);
    }
  }

  private ResourceMonitor getOrCreateResourceMonitor(String resourceName) {
    try {
      if (!_resourceMonitorMap.containsKey(resourceName)) {
        synchronized (_resourceMonitorMap) {
          if (!_resourceMonitorMap.containsKey(resourceName)) {
            String beanName = getResourceBeanName(resourceName);
            ResourceMonitor bean =
                new ResourceMonitor(_clusterName, resourceName, getObjectName(beanName));
            _resourceMonitorMap.put(resourceName, bean);
          }
        }
      }
    } catch (JMException ex) {
      LOG.error("Fail to register resource mbean, resource: " + resourceName);
    }

    return _resourceMonitorMap.get(resourceName);
  }

  public void resetMaxMissingTopStateGauge() {
    for (ResourceMonitor monitor : _resourceMonitorMap.values()) {
      monitor.resetMaxTopStateHandoffGauge();
    }
  }

  public void addMessageQueueSize(String instanceName, long msgQueueSize) {
    _instanceMsgQueueSizes.put(instanceName, msgQueueSize);
  }

  public void active() {
    LOG.info("Active ClusterStatusMonitor");
    try {
      register(this, getObjectName(clusterBeanName()));
    } catch (Exception e) {
      LOG.error("Fail to register ClusterStatusMonitor", e);
    }
  }

  public void reset() {
    LOG.info("Reset ClusterStatusMonitor");
    try {
      unregisterAllResources();
      _instanceMsgQueueSizes.clear();
      unregisterAllInstances();
      unregisterAllPerInstanceResources();
      unregister(getObjectName(clusterBeanName()));
      unregisterAllEventMonitors();
      unregisterAllWorkflowsMonitor();
      unregisterAllJobs();

      _rebalanceFailure = false;
    } catch (Exception e) {
      LOG.error("Fail to reset ClusterStatusMonitor, cluster: " + _clusterName, e);
    }
  }

  public void refreshWorkflowsStatus(WorkflowControllerDataProvider cache) {
    for (Map.Entry<String, WorkflowMonitor> workflowMonitor : _perTypeWorkflowMonitorMap
        .entrySet()) {
      workflowMonitor.getValue().resetGauges();
    }

    Map<String, WorkflowConfig> workflowConfigMap = cache.getWorkflowConfigMap();
    for (String workflow : workflowConfigMap.keySet()) {
      if (workflowConfigMap.get(workflow).isRecurring() || workflow.isEmpty()) {
        continue;
      }
      WorkflowContext workflowContext = cache.getWorkflowContext(workflow);
      TaskState currentState =
          workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getWorkflowState();
      updateWorkflowGauges(workflowConfigMap.get(workflow), currentState);
    }
  }
  public void updateWorkflowCounters(WorkflowConfig workflowConfig, TaskState to) {
    updateWorkflowCounters(workflowConfig, to, -1L);
  }

  public void updateWorkflowCounters(WorkflowConfig workflowConfig, TaskState to, long latency) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    WorkflowMonitor workflowMonitor = _perTypeWorkflowMonitorMap.get(workflowType);
    if (workflowMonitor != null) {
      workflowMonitor.updateWorkflowCounters(to, latency);
    }
  }

  private void updateWorkflowGauges(WorkflowConfig workflowConfig, TaskState current) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    WorkflowMonitor workflowMonitor = _perTypeWorkflowMonitorMap.get(workflowType);
    if (workflowMonitor != null) {
      workflowMonitor.updateWorkflowGauges(current);
    }
  }

  private String preProcessWorkflow(String workflowType) {
    if (workflowType == null || workflowType.length() == 0) {
      workflowType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    synchronized (_perTypeWorkflowMonitorMap) {
      if (!_perTypeWorkflowMonitorMap.containsKey(workflowType)) {
        WorkflowMonitor monitor = new WorkflowMonitor(_clusterName, workflowType);
        try {
          registerWorkflow(monitor);
        } catch (MalformedObjectNameException e) {
          LOG.error("Failed to register object for workflow type : " + workflowType, e);
        }
        _perTypeWorkflowMonitorMap.put(workflowType, monitor);
      }
    }
    return workflowType;
  }

  public void refreshJobsStatus(WorkflowControllerDataProvider cache) {
    for (Map.Entry<String, JobMonitor> jobMonitor : _perTypeJobMonitorMap.entrySet()) {
      jobMonitor.getValue().resetJobGauge();
    }
    for (String workflow : cache.getWorkflowConfigMap().keySet()) {
      if (workflow.isEmpty()) {
        continue;
      }
      WorkflowConfig workflowConfig = cache.getWorkflowConfig(workflow);
      if (workflowConfig == null) {
        continue;
      }
      Set<String> allJobs = workflowConfig.getJobDag().getAllNodes();
      WorkflowContext workflowContext = cache.getWorkflowContext(workflow);
      for (String job : allJobs) {
        TaskState currentState =
            workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getJobState(job);
        updateJobGauges(
            workflowConfig.getJobTypes() == null ? null : workflowConfig.getJobTypes().get(job),
            currentState);
      }
    }
  }

  public void updateJobCounters(JobConfig jobConfig, TaskState to) {
    updateJobCounters(jobConfig, to, -1L);
  }

  public void updateJobCounters(JobConfig jobConfig, TaskState to, long latency) {
    String jobType = jobConfig.getJobType();
    jobType = preProcessJobMonitor(jobType);
    JobMonitor jobMonitor = _perTypeJobMonitorMap.get(jobType);
    if (jobMonitor != null) {
      jobMonitor.updateJobCounters(to, latency);
    }
  }

  private void updateJobGauges(String jobType, TaskState current) {
    // When first time for WorkflowRebalancer call, jobconfig may not ready.
    // Thus only check it for gauge.
    jobType = preProcessJobMonitor(jobType);
    JobMonitor jobMonitor = _perTypeJobMonitorMap.get(jobType);
    if (jobMonitor != null) {
      jobMonitor.updateJobGauge(current);
    }
  }

  private String preProcessJobMonitor(String jobType) {
    if (jobType == null || jobType.length() == 0) {
      jobType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    synchronized (_perTypeJobMonitorMap) {
      if (!_perTypeJobMonitorMap.containsKey(jobType)) {
        JobMonitor monitor = new JobMonitor(_clusterName, jobType);
        try {
          registerJob(monitor);
        } catch (MalformedObjectNameException e) {
          LOG.error("Failed to register job type : " + jobType, e);
        }
        _perTypeJobMonitorMap.put(jobType, monitor);
      }
    }
    return jobType;
  }

  private void registerInstances(Collection<InstanceMonitor> instances)
      throws MalformedObjectNameException {
    synchronized (_instanceMonitorMap) {
      for (InstanceMonitor monitor : instances) {
        String instanceName = monitor.getInstanceName();
        String beanName = getInstanceBeanName(instanceName);
        register(monitor, getObjectName(beanName));
        _instanceMonitorMap.put(instanceName, monitor);
      }
    }
  }

  private void unregisterAllInstances() throws MalformedObjectNameException {
    synchronized (_instanceMonitorMap) {
      unregisterInstances(_instanceMonitorMap.keySet());
    }
  }

  private void unregisterInstances(Collection<String> instances) throws MalformedObjectNameException {
    synchronized (_instanceMonitorMap) {
      for (String instanceName : instances) {
        String beanName = getInstanceBeanName(instanceName);
        unregister(getObjectName(beanName));
      }
      _instanceMonitorMap.keySet().removeAll(instances);
    }
  }

  private void registerResources(Collection<String> resources) throws JMException {
    synchronized (_resourceMonitorMap) {
      for (String resourceName : resources) {
        ResourceMonitor monitor = _resourceMonitorMap.get(resourceName);
        if (monitor != null) {
          monitor.register();
        }
      }
    }
  }

  private void unregisterAllResources() {
    synchronized (_resourceMonitorMap) {
      unregisterResources(_resourceMonitorMap.keySet());
    }
  }

  private void unregisterResources(Collection<String> resources) {
    synchronized (_resourceMonitorMap) {
      for (String resourceName : resources) {
        ResourceMonitor monitor = _resourceMonitorMap.get(resourceName);
        if (monitor != null) {
          monitor.unregister();
        }
      }
      _resourceMonitorMap.keySet().removeAll(resources);
    }
  }

  private void unregisterAllEventMonitors() {
    synchronized (_clusterEventMonitorMap) {
      for (ClusterEventMonitor monitor : _clusterEventMonitorMap.values()) {
        monitor.unregister();
      }
      _clusterEventMonitorMap.clear();
    }
  }

  private void registerPerInstanceResources(Collection<PerInstanceResourceMonitor> monitors)
      throws MalformedObjectNameException {
    synchronized (_perInstanceResourceMonitorMap) {
      for (PerInstanceResourceMonitor monitor : monitors) {
        String instanceName = monitor.getInstanceName();
        String resourceName = monitor.getResourceName();
        String beanName = getPerInstanceResourceBeanName(instanceName, resourceName);
        register(monitor, getObjectName(beanName));
        _perInstanceResourceMonitorMap
            .put(new PerInstanceResourceMonitor.BeanName(instanceName, resourceName), monitor);
      }
    }
  }

  private void unregisterAllPerInstanceResources()
      throws MalformedObjectNameException {
    synchronized (_perInstanceResourceMonitorMap) {
      unregisterPerInstanceResources(_perInstanceResourceMonitorMap.keySet());
    }
  }

  private void unregisterPerInstanceResources(Collection<PerInstanceResourceMonitor.BeanName> beanNames)
      throws MalformedObjectNameException {
    synchronized (_perInstanceResourceMonitorMap) {
      for (PerInstanceResourceMonitor.BeanName beanName : beanNames) {
        unregister(getObjectName(getPerInstanceResourceBeanName(beanName.instanceName(), beanName.resourceName())));
      }
      _perInstanceResourceMonitorMap.keySet().removeAll(beanNames);
    }
  }

  private void registerWorkflow(WorkflowMonitor workflowMonitor) throws MalformedObjectNameException {
    String workflowBeanName = getWorkflowBeanName(workflowMonitor.getWorkflowType());
    register(workflowMonitor, getObjectName(workflowBeanName));
  }

  private void unregisterAllWorkflowsMonitor()
      throws MalformedObjectNameException {
    synchronized (_perTypeWorkflowMonitorMap) {
      Iterator<Map.Entry<String, WorkflowMonitor>> workflowIter =
          _perTypeWorkflowMonitorMap.entrySet().iterator();
      while (workflowIter.hasNext()) {
        Map.Entry<String, WorkflowMonitor> workflowEntry = workflowIter.next();
        unregister(getObjectName(getWorkflowBeanName(workflowEntry.getKey())));
        workflowIter.remove();
      }
    }
  }

  private void registerJob(JobMonitor jobMonitor) throws MalformedObjectNameException {
    String jobBeanName = getJobBeanName(jobMonitor.getJobType());
    register(jobMonitor, getObjectName(jobBeanName));
  }

  private void unregisterAllJobs() throws MalformedObjectNameException {
    synchronized (_perTypeJobMonitorMap) {
      Iterator<Map.Entry<String, JobMonitor>> jobIter = _perTypeJobMonitorMap.entrySet().iterator();
      while (jobIter.hasNext()) {
        Map.Entry<String, JobMonitor> jobEntry = jobIter.next();
        unregister(getObjectName(getJobBeanName(jobEntry.getKey())));
        jobIter.remove();
      }
    }
  }

  // For test only
  protected ResourceMonitor getResourceMonitor(String resourceName) {
    return _resourceMonitorMap.get(resourceName);
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
    return String.format("%s,%s", clusterBeanName(),
        new PerInstanceResourceMonitor.BeanName(instanceName, resourceName).toString());
  }

  /**
   * Build workflow per type bean name
   * "cluster={clusterName},workflowType={workflowType},
   * @param workflowType The workflow type
   * @return per workflow type bean name
   */
  public String getWorkflowBeanName(String workflowType) {
    return String.format("%s, %s=%s", clusterBeanName(), WORKFLOW_TYPE_DN_KEY, workflowType);
  }

  /**
   * Build job per type bean name
   * "cluster={clusterName},jobType={jobType},
   * @param jobType The job type
   * @return per job type bean name
   */
  public String getJobBeanName(String jobType) {
    return String.format("%s, %s=%s", clusterBeanName(), JOB_TYPE_DN_KEY, jobType);
  }

  @Override
  public String getSensorName() {
    return MonitorDomainNames.ClusterStatus.name() + "." + _clusterName;
  }

  @Override
  public long getEnabled() {
    return _enabled ? 1 : 0;
  }

  @Override
  public long getMaintenance() {
    return _inMaintenance ? 1 : 0;
  }

  public void setMaintenance(boolean inMaintenance) {
    _inMaintenance = inMaintenance;
  }

  @Override
  public long getPaused() {
    return _paused ? 1 : 0;
  }

  public void setPaused(boolean paused) {
    _paused = paused;
  }

  public void setEnabled(boolean enabled) {
    this._enabled = enabled;
  }

  public void reportRebalanceFailure() {
    _rebalanceFailureCount.incrementAndGet();
  }

  @Override
  public long getRebalanceFailureCounter() {
    return _rebalanceFailureCount.get();
  }

  @Override
  public long getTotalResourceGauge() {
    return _resourceMonitorMap.size();
  }

  @Override
  public long getTotalPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getPartitionGauge();
    }
    return total;
  }

  @Override
  public long getErrorPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getErrorPartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingTopStatePartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingTopStatePartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingMinActiveReplicaPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingMinActiveReplicaPartitionGauge();
    }
    return total;
  }

  @Override
  public long getMissingReplicaPartitionGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getMissingReplicaPartitionGauge();
    }
    return total;
  }

  @Override
  public long getDifferenceWithIdealStateGauge() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getDifferenceWithIdealStateGauge();
    }
    return total;
  }

  @Override
  public long getStateTransitionCounter() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getTotalMessageReceived();
    }
    return total;
  }

  @Override
  public long getPendingStateTransitionGuage() {
    long total = 0;
    for (Map.Entry<String, ResourceMonitor> entry : _resourceMonitorMap.entrySet()) {
      total += entry.getValue().getNumPendingStateTransitionGauge();
    }
    return total;
  }
}
