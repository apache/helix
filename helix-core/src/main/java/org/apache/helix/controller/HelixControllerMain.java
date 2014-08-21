package org.apache.helix.controller;

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

/**
 * start cluster manager controller
 * cluster manager controller has two modes:
 * 1) stand-alone mode: in this mode each controller gets a list of clusters
 *  and competes via leader election to become the controller for any of the clusters.
 *  if a controller fails to become the leader of a given cluster, it remains as a standby
 *  and re-does the leader election when the current leader fails
 *
 * 2) distributed mode: in this mode each controller first joins as participant into
 *   a special CONTROLLER_CLUSTER. Leader election happens in this special
 *   cluster. The one that becomes the leader controls all controllers (including itself
 *   to become leaders of other clusters.
 */

import java.util.Arrays;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.helix.participant.MultiClusterControllerTransitionHandlerFactory;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class HelixControllerMain {
  public static final String zkServerAddress = "zkSvr";
  public static final String cluster = "cluster";
  public static final String help = "help";
  public static final String mode = "mode";
  public static final String name = "controllerName";
  public static final String STANDALONE = "STANDALONE";
  public static final String DISTRIBUTED = "DISTRIBUTED";
  private static final Logger logger = Logger.getLogger(HelixControllerMain.class);

  // hack: OptionalBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServerAddress).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option modeOption =
        OptionBuilder
            .withLongOpt(mode)
            .withDescription(
                "Provide cluster controller mode (Optional): STANDALONE (default) or DISTRIBUTED")
            .create();
    modeOption.setArgs(1);
    modeOption.setRequired(false);
    modeOption.setArgName("Cluster controller mode (Optional)");

    Option controllerNameOption =
        OptionBuilder.withLongOpt(name)
            .withDescription("Provide cluster controller name (Optional)").create();
    controllerNameOption.setArgs(1);
    controllerNameOption.setRequired(false);
    controllerNameOption.setArgName("Cluster controller name (Optional)");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(modeOption);
    options.addOption(controllerNameOption);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + HelixControllerMain.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      logger.error("fail to parse command-line options. cliArgs: " + Arrays.toString(cliArgs), pe);
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static void addListenersToController(HelixManager manager,
      GenericHelixController controller) {
    try {
      manager.addInstanceConfigChangeListener(controller);
      manager.addConfigChangeListener(controller, ConfigScopeProperty.RESOURCE);
      manager.addConfigChangeListener(controller, ConfigScopeProperty.CONSTRAINT);
      manager.addLiveInstanceChangeListener(controller);
      manager.addIdealStateChangeListener(controller);
      // no need for controller to listen on external-view
      // manager.addExternalViewChangeListener(controller);
      manager.addControllerListener(controller);
    } catch (ZkInterruptedException e) {
      logger
          .warn("zk connection is interrupted during HelixManagerMain.addListenersToController(). "
              + e);
    } catch (Exception e) {
      logger.error("Error when creating HelixManagerContollerMonitor", e);
    }
  }

  public static HelixManager startHelixController(final String zkConnectString,
      final String clusterName, final String controllerName, final String controllerMode) {
    HelixManager manager = null;
    try {
      if (controllerMode.equalsIgnoreCase(STANDALONE)) {
        manager =
            HelixManagerFactory.getZKHelixManager(clusterName, controllerName,
                InstanceType.CONTROLLER, zkConnectString);
        manager.connect();
      } else if (controllerMode.equalsIgnoreCase(DISTRIBUTED)) {
        manager =
            HelixManagerFactory.getZKHelixManager(clusterName, controllerName,
                InstanceType.CONTROLLER_PARTICIPANT, zkConnectString);

        MultiClusterControllerTransitionHandlerFactory stateModelFactory =
            new MultiClusterControllerTransitionHandlerFactory(zkConnectString);

        StateMachineEngine stateMach = manager.getStateMachineEngine();
        stateMach.registerStateModelFactory(StateModelDefId.LeaderStandby, stateModelFactory);
        manager.connect();
      } else {
        logger.error("cluster controller mode:" + controllerMode + " NOT supported");
      }
    } catch (Exception e) {
      logger.error("Exception while starting controller", e);
    }

    return manager;
  }

  public static void main(String[] args) throws Exception {
    // read the config;
    // check if the this process is the master wait indefinitely
    // other approach is always process the events but when updating the zk
    // check if this is master.
    // This is difficult to get right
    // get the clusters to manage
    // for each cluster create a manager
    // add the respective listeners for each manager
    CommandLine cmd = processCommandLineArgs(args);
    String zkConnectString = cmd.getOptionValue(zkServerAddress);
    String clusterName = cmd.getOptionValue(cluster);
    String controllerMode = STANDALONE;
    String controllerName = null;

    if (cmd.hasOption(mode)) {
      controllerMode = cmd.getOptionValue(mode);
    }

    if (controllerMode.equalsIgnoreCase(DISTRIBUTED) && !cmd.hasOption(name)) {
      throw new IllegalArgumentException(
          "A unique cluster controller name is required in DISTRIBUTED mode");
    }

    controllerName = cmd.getOptionValue(name);

    // Espresso_driver.py will consume this
    logger.info("Cluster manager started, zkServer: " + zkConnectString + ", clusterName:"
        + clusterName + ", controllerName:" + controllerName + ", mode:" + controllerMode);

    HelixManager manager =
        startHelixController(zkConnectString, clusterName, controllerName, controllerMode);

    Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(manager));

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      logger.info("controller:" + controllerName + ", " + Thread.currentThread().getName()
          + " interrupted");
    } finally {
      manager.disconnect();
    }

  }
}
