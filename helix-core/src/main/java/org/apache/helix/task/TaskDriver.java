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
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.log4j.Logger;

import com.beust.jcommander.internal.Lists;

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
  private enum DriverCommand {
    start,
    stop,
    delete,
    resume,
    list
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
        driver.setTaskTargetState(resource, TargetState.STOP);
        break;
      case resume:
        driver.setTaskTargetState(resource, TargetState.START);
        break;
      case delete:
        driver.setTaskTargetState(resource, TargetState.DELETE);
        break;
      case list:
        driver.list(resource);
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
    _admin.setConfig(TaskUtil.getResourceConfigScope(_clusterName, flowName),
        flow.getResourceConfigMap());

    // then schedule tasks
    for (String job : flow.getJobConfigs().keySet()) {
      JobConfig.Builder builder = JobConfig.Builder.fromMap(flow.getJobConfigs().get(job));
      if (flow.getTaskConfigs() != null && flow.getTaskConfigs().containsKey(job)) {
        builder.addTaskConfigs(flow.getTaskConfigs().get(job));
      }
      scheduleJob(job, builder.build());
    }
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
    builder.setRebalancerMode(IdealState.RebalanceMode.USER_DEFINED);
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

  /** Public method to resume a job/workflow */
  public void resume(String resource) {
    setTaskTargetState(resource, TargetState.START);
  }

  /** Public method to stop a job/workflow */
  public void stop(String resource) {
    setTaskTargetState(resource, TargetState.STOP);
  }

  /** Public method to delete a job/workflow */
  public void delete(String resource) {
    setTaskTargetState(resource, TargetState.DELETE);
  }

  /** Helper function to change target state for a given task */
  private void setTaskTargetState(String jobResource, TargetState state) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    HelixProperty p = new HelixProperty(jobResource);
    p.getRecord().setSimpleField(WorkflowConfig.TARGET_STATE, state.name());
    accessor.updateProperty(accessor.keyBuilder().resourceConfig(jobResource), p);

    invokeRebalance();
  }

  public void list(String resource) {
    WorkflowConfig wCfg = TaskUtil.getWorkflowCfg(_manager, resource);
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_manager, resource);

    LOG.info("Workflow " + resource + " consists of the following tasks: "
        + wCfg.getJobDag().getAllNodes());
    LOG.info("Current state of workflow is " + wCtx.getWorkflowState().name());
    LOG.info("Job states are: ");
    LOG.info("-------");
    for (String job : wCfg.getJobDag().getAllNodes()) {
      LOG.info("Task " + job + " is " + wCtx.getJobState(job));

      // fetch task information
      JobContext jCtx = TaskUtil.getJobContext(_manager, job);

      // calculate taskPartitions
      List<Integer> partitions = Lists.newArrayList(jCtx.getPartitionSet());
      Collections.sort(partitions);

      // group partitions by status
      Map<TaskPartitionState, Integer> statusCount = new TreeMap<TaskPartitionState, Integer>();
      for (Integer i : partitions) {
        TaskPartitionState s = jCtx.getPartitionState(i);
        if (!statusCount.containsKey(s)) {
          statusCount.put(s, 0);
        }
        statusCount.put(s, statusCount.get(s) + 1);
      }

      for (TaskPartitionState s : statusCount.keySet()) {
        LOG.info(statusCount.get(s) + "/" + partitions.size() + " in state " + s.name());
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
      if (is.getStateModelDefRef().equals(TaskConstants.STATE_MODEL_NAME)) {
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
