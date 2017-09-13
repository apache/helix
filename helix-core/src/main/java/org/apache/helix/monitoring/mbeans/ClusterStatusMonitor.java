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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.model.*;
import org.apache.helix.task.*;
import org.apache.log4j.Logger;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class ClusterStatusMonitor implements ClusterStatusMonitorMBean {
  private static final Logger LOG = Logger.getLogger(ClusterStatusMonitor.class);

  static final String MESSAGE_QUEUE_STATUS_KEY = "MessageQueueStatus";
  static final String RESOURCE_STATUS_KEY = "ResourceStatus";
  public static final String PARTICIPANT_STATUS_KEY = "ParticipantStatus";
  static final String CLUSTER_DN_KEY = "cluster";
  static final String RESOURCE_DN_KEY = "resourceName";
  static final String INSTANCE_DN_KEY = "instanceName";
  static final String MESSAGE_QUEUE_DN_KEY = "messageQueue";
  static final String WORKFLOW_TYPE_DN_KEY = "workflowType";
  static final String JOB_TYPE_DN_KEY = "jobType";
  static final String DEFAULT_WORKFLOW_JOB_TYPE = "DEFAULT";

  public static final String DEFAULT_TAG = "DEFAULT";

  private final String _clusterName;
  private final MBeanServer _beanServer;

  private boolean _enabled = true;
  private Set<String> _liveInstances = Collections.emptySet();
  private Set<String> _instances = Collections.emptySet();
  private Set<String> _disabledInstances = Collections.emptySet();
  private Map<String, Map<String, List<String>>> _disabledPartitions = Collections.emptyMap();
  private Map<String, List<String>> _oldDisabledPartitions = Collections.emptyMap();
  private Map<String, Long> _instanceMsgQueueSizes = Maps.newConcurrentMap();

  private final ConcurrentHashMap<String, ResourceMonitor> _resourceMbeanMap = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, InstanceMonitor> _instanceMbeanMap = new ConcurrentHashMap<>();

  // phaseName -> eventMonitor
  private final ConcurrentHashMap<String, ClusterEventMonitor> _clusterEventMbeanMap =
      new ConcurrentHashMap<>();

  /**
   * PerInstanceResource bean map: beanName->bean
   */
  private final Map<PerInstanceResourceMonitor.BeanName, PerInstanceResourceMonitor> _perInstanceResourceMap =
      new ConcurrentHashMap<>();

  private final Map<String, WorkflowMonitor> _perTypeWorkflowMonitorMap = new ConcurrentHashMap<>();

  private final Map<String, JobMonitor> _perTypeJobMonitorMap = new ConcurrentHashMap<>();

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
  public synchronized void setClusterInstanceStatus(Set<String> liveInstanceSet, Set<String> instanceSet,
      Set<String> disabledInstanceSet, Map<String, Map<String, List<String>>> disabledPartitions,
      Map<String, List<String>> oldDisabledPartitions, Map<String, Set<String>> tags) {
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
          oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName),
          !disabledInstanceSet.contains(instanceName));
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
      if (_instanceMbeanMap.containsKey(instanceName)) {
        // Update the bean
        InstanceMonitor bean = _instanceMbeanMap.get(instanceName);
        String oldSensorName = bean.getSensorName();
        bean.updateInstance(tags.get(instanceName), disabledPartitions.get(instanceName),
            oldDisabledPartitions.get(instanceName), liveInstanceSet.contains(instanceName),
            !disabledInstanceSet.contains(instanceName));

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
   * Update the duration of handling a cluster event in a certain phase.
   *
   * @param phase
   * @param duration
   */
  public void updateClusterEventDuration(String phase, long duration) {
    if (!_clusterEventMbeanMap.containsKey(phase)) {
      ClusterEventMonitor monitor = new ClusterEventMonitor(this, phase);
      try {
        ClusterEventMonitor prevEventMbean = _clusterEventMbeanMap.put(phase, monitor);
        if (prevEventMbean != null) {
          prevEventMbean.unregister();
        }
        monitor.register();
      } catch (JMException e) {
        LOG.error("Failed to register ClusterEventMonitorMbean for cluster " + _clusterName
            + " and phase type: " + phase, e);
        return;
      }
    }

    ClusterEventMonitor eventMbean = _clusterEventMbeanMap.get(phase);
    eventMbean.reportDuration(duration);
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
      if (_instanceMbeanMap.containsKey(instance)) {
        _instanceMbeanMap.get(instance).increaseMessageCount(messageCountPerInstance.get(instance));
      }
    }
    for (String resource : messageCountPerResource.keySet()) {
      if (_resourceMbeanMap.containsKey(resource)) {
        _resourceMbeanMap.get(resource).increaseMessageCount(messageCountPerResource.get(resource));
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
    synchronized (_perInstanceResourceMap) {
      // Unregister beans for per-instance resources that no longer exist
      Set<PerInstanceResourceMonitor.BeanName> toUnregister = Sets.newHashSet(_perInstanceResourceMap.keySet());
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
      for (PerInstanceResourceMonitor.BeanName beanName : _perInstanceResourceMap.keySet()) {
        PerInstanceResourceMonitor bean = _perInstanceResourceMap.get(beanName);
        String stateModelDefName = resourceMap.get(beanName.resourceName()).getStateModelDefRef();
        InstanceConfig config = instanceConfigMap.get(beanName.instanceName());
        bean.update(beanMap.get(beanName), Sets.newHashSet(config.getTags()), stateModelDefMap.get(stateModelDefName));
      }
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

  public void setResourceStatus(ExternalView externalView, IdealState idealState, StateModelDefinition stateModelDef) {
    try {
      ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(externalView.getId());

      if (resourceMonitor != null) {
        resourceMonitor.updateResource(externalView, idealState, stateModelDef);
      }
    } catch (Exception e) {
      LOG.error("Fail to set resource status, resource: " + idealState.getResourceName(), e);
    }
  }

  public synchronized void updateMissingTopStateDurationStats(String resourceName, long duration, boolean succeeded) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateStateHandoffStats(ResourceMonitor.MonitorState.TOP_STATE, duration, succeeded);
    }
  }

  public void updateRebalancerStats(String resourceName, long numPendingRecoveryRebalancePartitions,
      long numPendingLoadRebalancePartitions, long numRecoveryRebalanceThrottledPartitions,
      long numLoadRebalanceThrottledPartitions) {
    ResourceMonitor resourceMonitor = getOrCreateResourceMonitor(resourceName);

    if (resourceMonitor != null) {
      resourceMonitor.updateRebalancerStat(numPendingRecoveryRebalancePartitions, numPendingLoadRebalancePartitions,
          numRecoveryRebalanceThrottledPartitions, numLoadRebalanceThrottledPartitions);
    }
  }

  private ResourceMonitor getOrCreateResourceMonitor(String resourceName) {
    try {
      if (!_resourceMbeanMap.containsKey(resourceName)) {
        synchronized (this) {
          if (!_resourceMbeanMap.containsKey(resourceName)) {
            String beanName = getResourceBeanName(resourceName);
            ResourceMonitor bean =
                new ResourceMonitor(_clusterName, resourceName, getObjectName(beanName));
            bean.register();
            _resourceMbeanMap.put(resourceName, bean);
          }
        }
      }
    } catch (JMException ex) {
      LOG.error("Fail to register resource mbean, resource: " + resourceName);
    }

    return _resourceMbeanMap.get(resourceName);
  }

  public void resetMaxMissingTopStateGauge() {
    for (ResourceMonitor monitor : _resourceMbeanMap.values()) {
      monitor.resetMaxTopStateHandoffGauge();
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
      unregisterEventMonitors(_clusterEventMbeanMap.values());
      unregisterWorkflows(_perTypeWorkflowMonitorMap.keySet());
      unregisterJobs(_perTypeJobMonitorMap.keySet());
    } catch (Exception e) {
      LOG.error("Fail to reset ClusterStatusMonitor, cluster: " + _clusterName, e);
    }
  }

  public void refreshWorkflowsStatus(TaskDriver driver) {
    for (WorkflowMonitor workflowMonitor : _perTypeWorkflowMonitorMap.values()) {
      workflowMonitor.resetGauges();
    }

    Map<String, WorkflowConfig> workflowConfigMap = driver.getWorkflows();
    for (String workflow : workflowConfigMap.keySet()) {
      if (workflowConfigMap.get(workflow).isRecurring() || workflow.isEmpty()) {
        continue;
      }
      WorkflowContext workflowContext = driver.getWorkflowContext(workflow);
      TaskState currentState = workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getWorkflowState();
      updateWorkflowGauges(workflowConfigMap.get(workflow), currentState);
    }
  }

  public void updateWorkflowCounters(WorkflowConfig workflowConfig, TaskState to) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    _perTypeWorkflowMonitorMap.get(workflowType).updateWorkflowCounters(to);
  }

  private void updateWorkflowGauges(WorkflowConfig workflowConfig, TaskState current) {
    String workflowType = workflowConfig.getWorkflowType();
    workflowType = preProcessWorkflow(workflowType);
    _perTypeWorkflowMonitorMap.get(workflowType).updateWorkflowGauges(current);
  }

  private String preProcessWorkflow(String workflowType) {
    if (workflowType == null || workflowType.length() == 0) {
      workflowType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    if (!_perTypeWorkflowMonitorMap.containsKey(workflowType)) {
      WorkflowMonitor monitor = new WorkflowMonitor(_clusterName, workflowType);
      try {
        registerWorkflow(monitor);
      } catch (MalformedObjectNameException e) {
        LOG.error("Failed to register object for workflow type : " + workflowType, e);
      }
      _perTypeWorkflowMonitorMap.put(workflowType, monitor);
    }
    return workflowType;
  }

  public void refreshJobsStatus(TaskDriver driver) {
    for (JobMonitor jobMonitor : _perTypeJobMonitorMap.values()) {
      jobMonitor.resetJobGauge();
    }
    for (String workflow : driver.getWorkflows().keySet()) {
      if (workflow.isEmpty()) {
        continue;
      }
      WorkflowConfig workflowConfig = driver.getWorkflowConfig(workflow);
      if (workflowConfig == null) {
        continue;
      }
      Set<String> allJobs = workflowConfig.getJobDag().getAllNodes();
      WorkflowContext workflowContext = driver.getWorkflowContext(workflow);
      for (String job : allJobs) {
        TaskState currentState = workflowContext == null ? TaskState.NOT_STARTED : workflowContext.getJobState(job);
        updateJobGauges(workflowConfig.getJobTypes() == null ? null : workflowConfig.getJobTypes().get(job),
            currentState);
      }
    }
  }

  public void updateJobCounters(JobConfig jobConfig, TaskState to) {
    String jobType = jobConfig.getJobType();
    jobType = preProcessJobMonitor(jobType);
    _perTypeJobMonitorMap.get(jobType).updateJobCounters(to);
  }

  private void updateJobGauges(String jobType, TaskState current) {
    // When first time for WorkflowRebalancer call, jobconfig may not ready.
    // Thus only check it for gauge.
    jobType = preProcessJobMonitor(jobType);
    _perTypeJobMonitorMap.get(jobType).updateJobGauge(current);
  }

  private String preProcessJobMonitor(String jobType) {
    if (jobType == null || jobType.length() == 0) {
      jobType = DEFAULT_WORKFLOW_JOB_TYPE;
    }

    if (!_perTypeJobMonitorMap.containsKey(jobType)) {
      JobMonitor monitor = new JobMonitor(_clusterName, jobType);
      try {
        registerJob(monitor);
      } catch (MalformedObjectNameException e) {
        LOG.error("Failed to register job type : " + jobType, e);
      }
      _perTypeJobMonitorMap.put(jobType, monitor);
    }
    return jobType;
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

  private synchronized void unregisterInstances(Collection<String> instances) throws MalformedObjectNameException {
    for (String instanceName : instances) {
      String beanName = getInstanceBeanName(instanceName);
      unregister(getObjectName(beanName));
    }
    _instanceMbeanMap.keySet().removeAll(instances);
  }

  private synchronized void unregisterResources(Collection<String> resources) throws MalformedObjectNameException {
    for (String resourceName : resources) {
      ResourceMonitor monitor = _resourceMbeanMap.get(resourceName);
      if (monitor != null) {
        monitor.unregister();
      }
    }
    _resourceMbeanMap.keySet().removeAll(resources);
  }

  private synchronized void unregisterEventMonitors(Collection<ClusterEventMonitor> monitors)
      throws MalformedObjectNameException {
    for (ClusterEventMonitor monitor : monitors) {
      monitor.unregister();
    }
    _resourceMbeanMap.keySet().removeAll(monitors);
  }

  private synchronized void registerPerInstanceResources(Collection<PerInstanceResourceMonitor> monitors)
      throws MalformedObjectNameException {
    for (PerInstanceResourceMonitor monitor : monitors) {
      String instanceName = monitor.getInstanceName();
      String resourceName = monitor.getResourceName();
      String beanName = getPerInstanceResourceBeanName(instanceName, resourceName);
      register(monitor, getObjectName(beanName));
      _perInstanceResourceMap.put(new PerInstanceResourceMonitor.BeanName(instanceName, resourceName), monitor);
    }
  }

  private synchronized void unregisterPerInstanceResources(Collection<PerInstanceResourceMonitor.BeanName> beanNames)
      throws MalformedObjectNameException {
    for (PerInstanceResourceMonitor.BeanName beanName : beanNames) {
      unregister(getObjectName(getPerInstanceResourceBeanName(beanName.instanceName(), beanName.resourceName())));
    }
    _perInstanceResourceMap.keySet().removeAll(beanNames);
  }

  private synchronized void registerWorkflow(WorkflowMonitor workflowMonitor) throws MalformedObjectNameException {
    String workflowBeanName = getWorkflowBeanName(workflowMonitor.getWorkflowType());
    register(workflowMonitor, getObjectName(workflowBeanName));
  }

  private synchronized void unregisterWorkflows(Collection<String> workflowMonitors)
      throws MalformedObjectNameException {
    for (String workflowMonitor : workflowMonitors) {
      String workflowBeanName = getWorkflowBeanName(workflowMonitor);
      unregister(getObjectName(workflowBeanName));
      _perTypeWorkflowMonitorMap.remove(workflowMonitor);
    }
  }

  private synchronized void registerJob(JobMonitor jobMonitor) throws MalformedObjectNameException {
    String jobBeanName = getJobBeanName(jobMonitor.getJobType());
    register(jobMonitor, getObjectName(jobBeanName));
  }

  private synchronized void unregisterJobs(Collection<String> jobMonitors) throws MalformedObjectNameException {
    for (String jobMonitor : jobMonitors) {
      String jobBeanName = getJobBeanName(jobMonitor);
      unregister(getObjectName(jobBeanName));
      _perTypeJobMonitorMap.remove(jobMonitor);
    }
  }

  protected ResourceMonitor getResourceMonitor(String resourceName) {
    return _resourceMbeanMap.get(resourceName);
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

  public void setEnabled(boolean enabled) {
    this._enabled = enabled;
  }
}
