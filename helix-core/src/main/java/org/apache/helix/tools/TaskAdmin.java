package org.apache.helix.tools;

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

import com.google.common.collect.Lists;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.log4j.Logger;

/**
 * CLI for operating workflows and jobs.
 * This is a wrapper of TaskDriver instance to allow command line changes of workflows and jobs.
 */
public class TaskAdmin {
  /** For logging */
  private static final Logger LOG = Logger.getLogger(TaskAdmin.class);

  /** Required option name for Helix endpoint */
  private static final String ZK_ADDRESS = "zk";

  /** Required option name for cluster against which to run task */
  private static final String CLUSTER_NAME_OPTION = "cluster";

  /** Required option name for task resource within target cluster */
  private static final String RESOURCE_OPTION = "resource";

  /** Field for specifying a workflow file when starting a job */
  private static final String WORKFLOW_FILE_OPTION = "file";

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
    String workflow = cl.getOptionValue(RESOURCE_OPTION);

    if (zkAddr == null || clusterName == null || workflow == null) {
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
      TaskDriver.DriverCommand cmd = TaskDriver.DriverCommand.valueOf(args[0]);
      switch (cmd) {
      case start:
        if (cl.hasOption(WORKFLOW_FILE_OPTION)) {
          driver.start(Workflow.parse(new File(cl.getOptionValue(WORKFLOW_FILE_OPTION))));
        } else {
          throw new IllegalArgumentException("Workflow file is required to start flow.");
        }
        break;
      case stop:
        driver.stop(workflow);
        break;
      case resume:
        driver.resume(workflow);
        break;
      case delete:
        driver.delete(workflow);
        break;
      case list:
        list(driver, workflow);
        break;
      case flush:
        driver.flushQueue(workflow);
        break;
      case clean:
        driver.cleanupQueue(workflow);
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

  private static void list(TaskDriver taskDriver, String workflow) {
    WorkflowConfig wCfg = taskDriver.getWorkflowConfig(workflow);
    if (wCfg == null) {
      LOG.error("Workflow " + workflow + " does not exist!");
      return;
    }
    WorkflowContext wCtx = taskDriver.getWorkflowContext(workflow);

    LOG.info("Workflow " + workflow + " consists of the following tasks: " + wCfg.getJobDag()
        .getAllNodes());
    String workflowState =
        (wCtx != null) ? wCtx.getWorkflowState().name() : TaskState.NOT_STARTED.name();
    LOG.info("Current state of workflow is " + workflowState);
    LOG.info("Job states are: ");
    LOG.info("-------");
    for (String job : wCfg.getJobDag().getAllNodes()) {
      TaskState jobState = (wCtx != null) ? wCtx.getJobState(job) : TaskState.NOT_STARTED;
      LOG.info("Job " + job + " is " + jobState);

      // fetch job information
      JobConfig jCfg = taskDriver.getJobConfig(job);
      JobContext jCtx = taskDriver.getJobContext(job);
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

  /** Constructs options set for all basic control messages */
  private static Options constructOptions() {
    Options options = new Options();
    options.addOptionGroup(contructGenericRequiredOptionGroup());
    options.addOptionGroup(constructStartOptionGroup());
    return options;
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
    helpFormatter.printHelp("java " + TaskAdmin.class.getName() + " " + cmd, cliOptions);
  }
}
