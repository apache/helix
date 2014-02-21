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
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.log4j.Logger;

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
    for (String task : flow.getTaskConfigs().keySet()) {
      scheduleTask(task, TaskConfig.Builder.fromMap(flow.getTaskConfigs().get(task)).build());
    }
  }

  /** Posts new task to cluster */
  private void scheduleTask(String taskResource, TaskConfig taskConfig) throws Exception {
    // Set up task resource based on partitions provided, or from target resource
    int numPartitions;
    List<Integer> partitions = taskConfig.getTargetPartitions();
    String targetResource = taskConfig.getTargetResource();
    if (partitions != null && !partitions.isEmpty()) {
      numPartitions = partitions.size();
    } else if (targetResource != null) {
      numPartitions =
          _admin.getResourceIdealState(_clusterName, taskConfig.getTargetResource())
              .getPartitionSet().size();
    } else {
      numPartitions = 0;
    }
    _admin.addResource(_clusterName, taskResource, numPartitions, TaskConstants.STATE_MODEL_NAME);
    _admin.setConfig(TaskUtil.getResourceConfigScope(_clusterName, taskResource),
        taskConfig.getResourceConfigMap());

    // Push out new ideal state based on number of target partitions
    CustomModeISBuilder builder = new CustomModeISBuilder(taskResource);
    builder.setRebalancerMode(IdealState.RebalanceMode.USER_DEFINED);
    builder.setNumReplica(1);
    builder.setNumPartitions(numPartitions);
    builder.setStateModel(TaskConstants.STATE_MODEL_NAME);
    for (int i = 0; i < numPartitions; i++) {
      builder.add(taskResource + "_" + i);
    }
    IdealState is = builder.build();
    Class<? extends HelixRebalancer> rebalancerClass =
        (targetResource != null) ? TaskRebalancer.class : IndependentTaskRebalancer.class;
    is.setRebalancerClassName(rebalancerClass.getName());
    _admin.setResourceIdealState(_clusterName, taskResource, is);
  }

  /** Public method to resume a task/workflow */
  public void resume(String resource) {
    setTaskTargetState(resource, TargetState.START);
  }

  /** Public method to stop a task/workflow */
  public void stop(String resource) {
    setTaskTargetState(resource, TargetState.STOP);
  }

  /** Public method to delete a task/workflow */
  public void delete(String resource) {
    setTaskTargetState(resource, TargetState.DELETE);
  }

  /** Helper function to change target state for a given task */
  private void setTaskTargetState(String taskResource, TargetState state) {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    HelixProperty p = new HelixProperty(taskResource);
    p.getRecord().setSimpleField(WorkflowConfig.TARGET_STATE, state.name());
    accessor.updateProperty(accessor.keyBuilder().resourceConfig(taskResource), p);

    invokeRebalance();
  }

  public void list(String resource) {
    WorkflowConfig wCfg = TaskUtil.getWorkflowCfg(_manager, resource);
    WorkflowContext wCtx = TaskUtil.getWorkflowContext(_manager, resource);

    LOG.info("Workflow " + resource + " consists of the following tasks: "
        + wCfg.getTaskDag().getAllNodes());
    LOG.info("Current state of workflow is " + wCtx.getWorkflowState().name());
    LOG.info("Task states are: ");
    LOG.info("-------");
    for (String task : wCfg.getTaskDag().getAllNodes()) {
      LOG.info("Task " + task + " is " + wCtx.getTaskState(task));

      // fetch task information
      TaskContext tCtx = TaskUtil.getTaskContext(_manager, task);
      TaskConfig tCfg = TaskUtil.getTaskCfg(_manager, task);

      // calculate taskPartitions
      List<Integer> partitions;
      if (tCfg.getTargetPartitions() != null) {
        partitions = tCfg.getTargetPartitions();
      } else {
        partitions = new ArrayList<Integer>();
        for (String pStr : _admin.getResourceIdealState(_clusterName, tCfg.getTargetResource())
            .getPartitionSet()) {
          partitions
              .add(Integer.parseInt(pStr.substring(pStr.lastIndexOf("_") + 1, pStr.length())));
        }
      }

      // group partitions by status
      Map<TaskPartitionState, Integer> statusCount = new TreeMap<TaskPartitionState, Integer>();
      for (Integer i : partitions) {
        TaskPartitionState s = tCtx.getPartitionState(i);
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

  /** Constructs option group containing options required by all drivable tasks */
  private static OptionGroup contructGenericRequiredOptionGroup() {
    Option zkAddressOption =
        OptionBuilder.isRequired().hasArgs(1).withArgName("zkAddress").withLongOpt(ZK_ADDRESS)
            .withDescription("ZK address managing target cluster").create();

    Option clusterNameOption =
        OptionBuilder.isRequired().hasArgs(1).withArgName("clusterName")
            .withLongOpt(CLUSTER_NAME_OPTION).withDescription("Target cluster name").create();

    Option taskResourceOption =
        OptionBuilder.isRequired().hasArgs(1).withArgName("resourceName")
            .withLongOpt(RESOURCE_OPTION).withDescription("Target workflow or task").create();

    OptionGroup group = new OptionGroup();
    group.addOption(zkAddressOption);
    group.addOption(clusterNameOption);
    group.addOption(taskResourceOption);
    return group;
  }

  /** Constructs option group containing options required by all drivable tasks */
  private static OptionGroup constructStartOptionGroup() {
    Option workflowFileOption =
        OptionBuilder.withLongOpt(WORKFLOW_FILE_OPTION).hasArgs(1).withArgName("workflowFile")
            .withDescription("Local file describing workflow").create();

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
