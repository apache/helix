package org.apache.helix.task;

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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * CLI for scheduling/canceling workflows
 */
public class TaskDriver {
  /** For logging */
  private static final Logger LOG = Logger.getLogger(TaskDriver.class);

  /** Required option name for Helix endpoint */
  private static final String ZK_ADDRESS = "zk";

  /** Required option name for cluster against which to run task */
  private static final String CLUSTER_NAME_OPTION = "cluster";

  /** Required option name for task resource within target cluster */
  private static final String RESOURCE_OPTION = "resource";

  /** Field for specifying a workflow file when starting a job */
  private static final String WORKFLOW_FILE_OPTION = "file";

  private final HelixManager _manager;
  private final HelixAdmin _admin;
  private final String _clusterName;

  /** Commands which may be parsed from the first argument to main */
  public enum DriverCommand {
    start,
    stop,
    delete,
    resume,
    list,
    flush
  }

  public TaskDriver(HelixManager manager) {
    _manager = manager;
    _clusterName = manager.getClusterName();
    _admin = manager.getClusterManagmentTool();
  }

  /**
   * Parses the first argument as a driver command and the rest of the
   * arguments are parsed based on that command. Constructs a Helix
   * message and posts it to the controller
   */
  public static void main(String[] args) throws Exception {
    String[] cmdArgs = Arrays.copyOfRange(args, 1, args.length);
    CommandLine cl = parseOptions(cmdArgs, constructOptions(), args[0]);
    String zkAddr = cl.getOptionValue(ZK_ADDRESS);
    String clusterName = cl.getOptionValue(CLUSTER_NAME_OPTION);
    String resource = cl.getOptionValue(RESOURCE_OPTION);

    if (zkAddr == null || clusterName == null || resource == null) {
      printUsage(constructOptions(), "[cmd]");
      throw new IllegalArgumentException(
          "zk, cluster, and resource must all be non-null for all commands");
    }

    HelixManager helixMgr =
        HelixManagerFactory.getZKHelixManager(clusterName, "Admin", InstanceType.ADMINISTRATOR,
            zkAddr);
    helixMgr.connect();
    TaskDriver driver = new TaskDriver(helixMgr);
    try {
      DriverCommand cmd = DriverCommand.valueOf(args[0]);
      switch (cmd) {
      case start:
        if (cl.hasOption(WORKFLOW_FILE_OPTION)) {
          driver.start(Workflow.parse(new File(cl.getOptionValue(WORKFLOW_FILE_OPTION))));
        } else {
          throw new IllegalArgumentException("Workflow file is required to start flow.");
        }
        break;
      case stop:
        driver.setWorkflowTargetState(resource, TargetState.STOP);
        break;
      case resume:
        driver.setWorkflowTargetState(resource, TargetState.START);
        break;
      case delete:
        driver.setWorkflowTargetState(resource, TargetState.DELETE);
        break;
      case list:
        driver.list(resource);
        break;
      case flush:
        driver.flushQueue(resource);
        break;
      default:
        throw new IllegalArgumentException("Unknown command " + args[0]);
      }
    } catch (IllegalArgumentException e) {
      LOG.error("Unknown driver command " + args[0]);
      throw e;
    }

    helixMgr.disconnect();
  }

  /** Schedules a new workflow */
  public void start(Workflow flow) throws Exception {
    // TODO: check that namespace for workflow is available
    LOG.info("Starting workflow " + flow.getName());
    flow.validate();

    String flowName = flow.getName();

    // first, add workflow config to ZK
    _admin.setConfig(TaskUtil.getResourceConfigScope(_clusterName, flowName), flow
        .getWorkflowConfig().getResourceConfigMap());

    // then schedule jobs
    for (String job : flow.getJobConfigs().keySet()) {
      JobConfig.Builder builder = JobConfig.Builder.fromMap(flow.getJobConfigs().get(job));
      if (flow.getTaskConfigs() != null && flow.getTaskConfigs().containsKey(job)) {
        builder.addTaskConfigs(flow.getTaskConfigs().get(job));
      }
      scheduleJob(job, builder.build());
    }
  }

  /** Creates a new named job queue (workflow) */
  public void createQueue(JobQueue queue) throws Exception {
    String queueName = queue.getName();
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    HelixProperty property = new HelixProperty(queueName);
    property.getRecord().getSimpleFields().putAll(queue.getResourceConfigMap());
    boolean created =
        accessor.createProperty(accessor.keyBuilder().resourceConfig(queueName), property);
    if (!created) {
      throw new IllegalArgumentException("Queue " + queueName + " already exists!");
    }
  }

  /** Flushes a named job queue */
  public void flushQueue(String queueName) throws Exception {
    WorkflowConfig config = TaskUtil.getWorkflowCfg(_manager, queueName);
    if (config == null) {
      throw new IllegalArgumentException("Queue does not exist!");
    }

    // Remove all ideal states and resource configs to trigger a drop event
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final Set<String> toRemove = Sets.newHashSet(config.getJobDag().getAllNodes());
    for (String resourceName : toRemove) {
      accessor.removeProperty(keyBuilder.idealStates(resourceName));
      accessor.removeProperty(keyBuilder.resourceConfig(resourceName));
      // Delete context
      String contextKey = Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, resourceName);
      _manager.getHelixPropertyStore().remove(contextKey, AccessOption.PERSISTENT);
    }

    // Now atomically clear the DAG
    String path = keyBuilder.resourceConfig(queueName).getPath();
    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        JobDag jobDag = JobDag.fromJson(currentData.getSimpleField(WorkflowConfig.DAG));
        for (String resourceName : toRemove) {
          for (String child : jobDag.getDirectChildren(resourceName)) {
            jobDag.getChildrenToParents().get(child).remove(resourceName);
          }
          for (String parent : jobDag.getDirectParents(resourceName)) {
            jobDag.getParentsToChildren().get(parent).remove(resourceName);
          }
          jobDag.getChildrenToParents().remove(resourceName);
          jobDag.getParentsToChildren().remove(resourceName);
          jobDag.getAllNodes().remove(resourceName);
        }
        try {
          currentData.setSimpleField(WorkflowConfig.DAG, jobDag.toJson());
        } catch (Exception e) {
          throw new IllegalArgumentException(e);
        }
        return currentData;
      }
    };
    accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);

    // Now atomically clear the results
    path =
        Joiner.on("/")
            .join(TaskConstants.REBALANCER_CONTEXT_ROOT, queueName, TaskUtil.CONTEXT_NODE);
    updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        Map<String, String> states = currentData.getMapField(WorkflowContext.JOB_STATES);
        if (states != null) {
          states.keySet().removeAll(toRemove);
        }
        return currentData;
      }
    };
    _manager.getHelixPropertyStore().update(path, updater, AccessOption.PERSISTENT);
  }

  /** Delete a job from an existing named queue, the queue has to be stopped prior to this call */
  public void deleteJob(final String queueName, final String jobName) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    HelixProperty workflowConfig =
        accessor.getProperty(accessor.keyBuilder().resourceConfig(queueName));
    if (workflowConfig == null) {
      throw new IllegalArgumentException("Queue " + queueName + " does not yet exist!");
    }
    boolean isTerminable =
        workflowConfig.getRecord().getBooleanField(WorkflowConfig.TERMINABLE, true);
    if (isTerminable) {
      throw new IllegalArgumentException(queueName + " is not a queue!");
    }

    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_manager, queueName);
    String workflowState =
        (wCtx != null) ? wCtx.getWorkflowState().name() : TaskState.NOT_STARTED.name();

    if (workflowState.equals(TaskState.IN_PROGRESS)) {
      throw new IllegalStateException("Queue " + queueName + " is still in progress!");
    }

    // Remove the job from the queue in the DAG
    final String namespacedJobName = TaskUtil.getNamespacedJobName(queueName, jobName);
    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        // Add the node to the existing DAG
        JobDag jobDag = JobDag.fromJson(currentData.getSimpleField(WorkflowConfig.DAG));
        Set<String> allNodes = jobDag.getAllNodes();
        if (!allNodes.contains(namespacedJobName)) {
          throw new IllegalStateException("Could not delete job from queue " + queueName + ", job "
              + jobName + " not exists");
        }

        String parent = null;
        String child = null;
        // remove the node from the queue
        for (String node : allNodes) {
          if (!node.equals(namespacedJobName)) {
            if (jobDag.getDirectChildren(node).contains(namespacedJobName)) {
              parent = node;
              jobDag.removeParentToChild(parent, namespacedJobName);
            } else if (jobDag.getDirectParents(node).contains(namespacedJobName)) {
              child = node;
              jobDag.removeParentToChild(namespacedJobName, child);
            }
          }
        }

        if (parent != null && child != null) {
          jobDag.addParentToChild(parent, child);
        }

        jobDag.removeNode(namespacedJobName);

        // Save the updated DAG
        try {
          currentData.setSimpleField(WorkflowConfig.DAG, jobDag.toJson());
        } catch (Exception e) {
          throw new IllegalStateException("Could not remove job " + jobName + " from queue "
              + queueName, e);
        }
        return currentData;
      }
    };

    String path = accessor.keyBuilder().resourceConfig(queueName).getPath();
    boolean status = accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);
    if (!status) {
      throw new IllegalArgumentException("Could not enqueue job");
    }

    // delete the ideal state and resource config for the job
    _admin.dropResource(_clusterName, namespacedJobName);

    // update queue's property to remove job from JOB_STATES if it is already started.
    String queuePropertyPath =
        Joiner.on("/")
            .join(TaskConstants.REBALANCER_CONTEXT_ROOT, queueName, TaskUtil.CONTEXT_NODE);
    updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData != null) {
          Map<String, String> states = currentData.getMapField(WorkflowContext.JOB_STATES);
          if (states != null && states.containsKey(namespacedJobName)) {
            states.keySet().remove(namespacedJobName);
          }
        }
        return currentData;
      }
    };
    HelixPropertyStore<ZNRecord> propertyStore = _manager.getHelixPropertyStore();
    propertyStore.update(queuePropertyPath, updater, AccessOption.PERSISTENT);

    // Delete the job from property store
    String jobPropertyPath =
        Joiner.on("/").join(TaskConstants.REBALANCER_CONTEXT_ROOT, namespacedJobName);
    propertyStore.remove(jobPropertyPath, AccessOption.PERSISTENT);
  }

  /** Adds a new job to the end an existing named queue */
  public void enqueueJob(final String queueName, final String jobName, JobConfig.Builder jobBuilder)
      throws Exception {
    // Get the job queue config and capacity
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    HelixProperty workflowConfig =
        accessor.getProperty(accessor.keyBuilder().resourceConfig(queueName));
    if (workflowConfig == null) {
      throw new IllegalArgumentException("Queue " + queueName + " does not yet exist!");
    }
    boolean isTerminable =
        workflowConfig.getRecord().getBooleanField(WorkflowConfig.TERMINABLE, true);
    if (isTerminable) {
      throw new IllegalArgumentException(queueName + " is not a queue!");
    }
    final int capacity =
        workflowConfig.getRecord().getIntField(JobQueue.CAPACITY, Integer.MAX_VALUE);

    // Create the job to ensure that it validates
    JobConfig jobConfig = jobBuilder.setWorkflow(queueName).build();

    // Add the job to the end of the queue in the DAG
    final String namespacedJobName = TaskUtil.getNamespacedJobName(queueName, jobName);
    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        // Add the node to the existing DAG
        JobDag jobDag = JobDag.fromJson(currentData.getSimpleField(WorkflowConfig.DAG));
        Set<String> allNodes = jobDag.getAllNodes();
        if (allNodes.size() >= capacity) {
          throw new IllegalStateException("Queue " + queueName + " is at capacity, will not add "
              + jobName);
        }
        if (allNodes.contains(namespacedJobName)) {
          throw new IllegalStateException("Could not add to queue " + queueName + ", job "
              + jobName + " already exists");
        }
        jobDag.addNode(namespacedJobName);

        // Add the node to the end of the queue
        String candidate = null;
        for (String node : allNodes) {
          if (!node.equals(namespacedJobName) && jobDag.getDirectChildren(node).isEmpty()) {
            candidate = node;
            break;
          }
        }
        if (candidate != null) {
          jobDag.addParentToChild(candidate, namespacedJobName);
        }

        // Save the updated DAG
        try {
          currentData.setSimpleField(WorkflowConfig.DAG, jobDag.toJson());
        } catch (Exception e) {
          throw new IllegalStateException(
              "Could not add job " + jobName + " to queue " + queueName, e);
        }
        return currentData;
      }
    };
    String path = accessor.keyBuilder().resourceConfig(queueName).getPath();
    boolean status = accessor.getBaseDataAccessor().update(path, updater, AccessOption.PERSISTENT);
    if (!status) {
      throw new IllegalArgumentException("Could not enqueue job");
    }
    // Schedule the job
    scheduleJob(namespacedJobName, jobConfig);
  }

  /** Posts new job to cluster */
  private void scheduleJob(String jobResource, JobConfig jobConfig) throws Exception {
    // Set up job resource based on partitions from target resource
    int numIndependentTasks = jobConfig.getTaskConfigMap().size();
    int numPartitions =
        (numIndependentTasks > 0) ? numIndependentTasks : _admin
            .getResourceIdealState(_clusterName, jobConfig.getTargetResource()).getPartitionSet()
            .size();
    _admin.addResource(_clusterName, jobResource, numPartitions, TaskConstants.STATE_MODEL_NAME);

    // Set the job configuration
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    HelixProperty resourceConfig = new HelixProperty(jobResource);
    resourceConfig.getRecord().getSimpleFields().putAll(jobConfig.getResourceConfigMap());
    Map<String, TaskConfig> taskConfigMap = jobConfig.getTaskConfigMap();
    if (taskConfigMap != null) {
      for (TaskConfig taskConfig : taskConfigMap.values()) {
        resourceConfig.getRecord().setMapField(taskConfig.getId(), taskConfig.getConfigMap());
      }
    }
    accessor.setProperty(keyBuilder.resourceConfig(jobResource), resourceConfig);

    // Push out new ideal state based on number of target partitions
    CustomModeISBuilder builder = new CustomModeISBuilder(jobResource);
    builder.setRebalancerMode(IdealState.RebalanceMode.TASK);
    builder.setNumReplica(1);
    builder.setNumPartitions(numPartitions);
    builder.setStateModel(TaskConstants.STATE_MODEL_NAME);
    for (int i = 0; i < numPartitions; i++) {
      builder.add(jobResource + "_" + i);
    }
    IdealState is = builder.build();
    if (taskConfigMap != null && !taskConfigMap.isEmpty()) {
      is.setRebalancerClassName(GenericTaskRebalancer.class.getName());
    } else {
      is.setRebalancerClassName(FixedTargetTaskRebalancer.class.getName());
    }
    _admin.setResourceIdealState(_clusterName, jobResource, is);
  }

  /** Public method to resume a workflow/queue */
  public void resume(String workflow) {
    setWorkflowTargetState(workflow, TargetState.START);
  }

  /** Public method to stop a workflow/queue */
  public void stop(String workflow) {
    setWorkflowTargetState(workflow, TargetState.STOP);
  }

  /** Public method to delete a workflow/queue */
  public void delete(String workflow) {
    setWorkflowTargetState(workflow, TargetState.DELETE);
  }

  /** Helper function to change target state for a given workflow */
  private void setWorkflowTargetState(String workflowName, TargetState state) {
    setSingleWorkflowTargetState(workflowName, state);

    // For recurring schedules, child workflows must also be handled
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    List<String> resources = accessor.getChildNames(accessor.keyBuilder().resourceConfigs());
    String prefix = workflowName + "_" + TaskConstants.SCHEDULED;
    for (String resource : resources) {
      if (resource.startsWith(prefix)) {
        setSingleWorkflowTargetState(resource, state);
      }
    }
  }

  /** Helper function to change target state for a given workflow */
  private void setSingleWorkflowTargetState(String workflowName, final TargetState state) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        // Only update target state for non-completed workflows
        String finishTime = currentData.getSimpleField(WorkflowContext.FINISH_TIME);
        if (finishTime == null || finishTime.equals(WorkflowContext.UNFINISHED)) {
          currentData.setSimpleField(WorkflowConfig.TARGET_STATE, state.name());
        }
        return currentData;
      }
    };
    List<DataUpdater<ZNRecord>> updaters = Lists.newArrayList();
    updaters.add(updater);
    List<String> paths = Lists.newArrayList();
    paths.add(accessor.keyBuilder().resourceConfig(workflowName).getPath());
    accessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
    invokeRebalance();
  }

  public void list(String resource) {
    WorkflowConfig wCfg = TaskUtil.getWorkflowCfg(_manager, resource);
    if (wCfg == null) {
      LOG.error("Workflow " + resource + " does not exist!");
      return;
    }
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_manager, resource);

    LOG.info("Workflow " + resource + " consists of the following tasks: "
        + wCfg.getJobDag().getAllNodes());
    String workflowState =
        (wCtx != null) ? wCtx.getWorkflowState().name() : TaskState.NOT_STARTED.name();
    LOG.info("Current state of workflow is " + workflowState);
    LOG.info("Job states are: ");
    LOG.info("-------");
    for (String job : wCfg.getJobDag().getAllNodes()) {
      TaskState jobState = (wCtx != null) ? wCtx.getJobState(job) : TaskState.NOT_STARTED;
      LOG.info("Job " + job + " is " + jobState);

      // fetch job information
      JobConfig jCfg = TaskUtil.getJobCfg(_manager, job);
      JobContext jCtx = TaskUtil.getJobContext(_manager, job);
      if (jCfg == null || jCtx == null) {
        LOG.info("-------");
        continue;
      }

      // calculate taskPartitions
      List<Integer> partitions = Lists.newArrayList(jCtx.getPartitionSet());
      Collections.sort(partitions);

      // report status
      for (Integer partition : partitions) {
        String taskId = jCtx.getTaskIdForPartition(partition);
        taskId = (taskId != null) ? taskId : jCtx.getTargetForPartition(partition);
        LOG.info("Task: " + taskId);
        TaskConfig taskConfig = jCfg.getTaskConfig(taskId);
        if (taskConfig != null) {
          LOG.info("Configuration: " + taskConfig.getConfigMap());
        }
        TaskPartitionState state = jCtx.getPartitionState(partition);
        state = (state != null) ? state : TaskPartitionState.INIT;
        LOG.info("State: " + state);
        String assignedParticipant = jCtx.getAssignedParticipant(partition);
        if (assignedParticipant != null) {
          LOG.info("Assigned participant: " + assignedParticipant);
        }
        LOG.info("-------");
      }
      LOG.info("-------");
    }
  }

  /**
   * Hack to invoke rebalance until bug concerning resource config changes not driving rebalance is
   * fixed
   */
  public void invokeRebalance() {
    // find a task
    for (String resource : _admin.getResourcesInCluster(_clusterName)) {
      IdealState is = _admin.getResourceIdealState(_clusterName, resource);
      if (is != null && is.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
        HelixDataAccessor accessor = _manager.getHelixDataAccessor();
        accessor.updateProperty(accessor.keyBuilder().idealStates(resource), is);
        break;
      }
    }
  }

  /** Constructs options set for all basic control messages */
  private static Options constructOptions() {
    Options options = new Options();
    options.addOptionGroup(contructGenericRequiredOptionGroup());
    options.addOptionGroup(constructStartOptionGroup());
    return options;
  }

  /** Constructs option group containing options required by all drivable jobs */
  @SuppressWarnings("static-access")
  private static OptionGroup contructGenericRequiredOptionGroup() {
    Option zkAddressOption =
        OptionBuilder.isRequired().withLongOpt(ZK_ADDRESS)
            .withDescription("ZK address managing cluster").create();
    zkAddressOption.setArgs(1);
    zkAddressOption.setArgName("zkAddress");

    Option clusterNameOption =
        OptionBuilder.isRequired().withLongOpt(CLUSTER_NAME_OPTION).withDescription("Cluster name")
            .create();
    clusterNameOption.setArgs(1);
    clusterNameOption.setArgName("clusterName");

    Option taskResourceOption =
        OptionBuilder.isRequired().withLongOpt(RESOURCE_OPTION)
            .withDescription("Workflow or job name").create();
    taskResourceOption.setArgs(1);
    taskResourceOption.setArgName("resourceName");

    OptionGroup group = new OptionGroup();
    group.addOption(zkAddressOption);
    group.addOption(clusterNameOption);
    group.addOption(taskResourceOption);
    return group;
  }

  /** Constructs option group containing options required by all drivable jobs */
  private static OptionGroup constructStartOptionGroup() {
    @SuppressWarnings("static-access")
    Option workflowFileOption =
        OptionBuilder.withLongOpt(WORKFLOW_FILE_OPTION)
            .withDescription("Local file describing workflow").create();
    workflowFileOption.setArgs(1);
    workflowFileOption.setArgName("workflowFile");

    OptionGroup group = new OptionGroup();
    group.addOption(workflowFileOption);
    return group;
  }

  /** Attempts to parse options for given command, printing usage under failure */
  private static CommandLine parseOptions(String[] args, Options options, String cmdStr) {
    CommandLineParser cliParser = new GnuParser();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(options, args);
    } catch (ParseException pe) {
      LOG.error("CommandLineClient: failed to parse command-line options: " + pe.toString());
      printUsage(options, cmdStr);
      System.exit(1);
    }
    boolean ret = checkOptionArgsNumber(cmd.getOptions());
    if (!ret) {
      printUsage(options, cmdStr);
      System.exit(1);
    }

    return cmd;
  }

  /** Ensures options argument counts are correct */
  private static boolean checkOptionArgsNumber(Option[] options) {
    for (Option option : options) {
      int argNb = option.getArgs();
      String[] args = option.getValues();
      if (argNb == 0) {
        if (args != null && args.length > 0) {
          System.err.println(option.getArgName() + " shall have " + argNb + " arguments (was "
              + Arrays.toString(args) + ")");
          return false;
        }
      } else {
        if (args == null || args.length != argNb) {
          System.err.println(option.getArgName() + " shall have " + argNb + " arguments (was "
              + Arrays.toString(args) + ")");
          return false;
        }
      }
    }
    return true;
  }

  /** Displays CLI usage for given option set and command name */
  private static void printUsage(Options cliOptions, String cmd) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + TaskDriver.class.getName() + " " + cmd, cliOptions);
  }
}
