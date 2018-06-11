package org.apache.helix.tools.commandtools;

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

import java.util.Arrays;
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
import org.apache.helix.examples.BrokerResourceOnlineOfflineStateModelFactory;
import org.apache.helix.examples.LeaderStandbyStateModelFactory;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.examples.OnlineOfflineStateModelFactory;
import org.apache.helix.examples.SegmentOnlineOfflineStateModelFactory;
import org.apache.helix.manager.zk.HelixManagerShutdownHook;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleParticipant {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleParticipant.class);

  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String instances = "instances";
  public static final String help = "help";
  public static final String transDelay = "transDelay";

  private final String zkConnectString;
  private final String clusterName;
  private final String instanceName;
  private HelixManager manager;

  private StateModelFactory<StateModel> stateModelFactory;
  private final int delay;

  public ExampleParticipant(String zkConnectString, String clusterName, String instanceName,
      int delay) {
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this.delay = delay;
  }

  public void start() throws Exception {
    manager =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkConnectString);

    // genericStateMachineHandler = new StateMachineEngine();
    // genericStateMachineHandler.registerStateModelFactory(stateModelType,
    // stateModelFactory);

    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory("MasterSlave",
        new MasterSlaveStateModelFactory(this.instanceName, delay));
    stateMach.registerStateModelFactory("OnlineOffline",
        new OnlineOfflineStateModelFactory(this.instanceName, delay));
    stateMach.registerStateModelFactory("LeaderStandby",
        new LeaderStandbyStateModelFactory(this.instanceName, delay));
    stateMach.registerStateModelFactory("BrokerResourceOnlineOfflineStateModel",
        new BrokerResourceOnlineOfflineStateModelFactory());
    stateMach.registerStateModelFactory("SegmentOnlineOfflineStateModel",
        new SegmentOnlineOfflineStateModelFactory());

    manager.connect();
    manager.getMessagingService()
        .registerMessageHandlerFactory(MessageType.STATE_TRANSITION.name(), stateMach);
  }

  public void stop() {
    manager.disconnect();
  }

  public HelixManager getManager() {
    return manager;
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServer).withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option instancesOption =
        OptionBuilder.withLongOpt(instances).withDescription("Provide instance names, separated by ':").create();
    instancesOption.setArgs(1);
    instancesOption.setRequired(true);
    instancesOption.setArgName("Instance names (Required)");

    Option transDelayOption =
        OptionBuilder.withLongOpt(transDelay).withDescription("Provide state trans delay").create();
    transDelayOption.setArgs(1);
    transDelayOption.setRequired(false);
    transDelayOption.setArgName("Delay time in state transition, in MS");

    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(clusterOption);
    options.addOption(instancesOption);
    options.addOption(transDelayOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ExampleParticipant.class.getName(), cliOptions);
  }

  public static void main(String[] args) throws Exception {
    int delay = 0;

    CommandLine cmd = ToolsUtil.processCommandLineArgs(args, constructCommandLineOptions());
    String zkConnectString = cmd.getOptionValue(zkServer);
    String clusterName = cmd.getOptionValue(cluster);
    String instanceNames = cmd.getOptionValue(instances);
    List<String> hosts = Arrays.asList(instanceNames.split(":"));

    if (cmd.hasOption(transDelay)) {
      try {
        delay = Integer.parseInt(cmd.getOptionValue(transDelay));
        if (delay < 0) {
          throw new Exception("delay must be positive");
        }
      } catch (Exception e) {
        e.printStackTrace();
        delay = 0;
      }
    }

    System.out.println("Starting Instances with ZK:" + zkConnectString + ", cluster: " + clusterName
        + ", instances: " + hosts);

    for (String instanceName : hosts) {
      System.out.println("Starting Instance:" + instanceName);
      ExampleParticipant process =
          new ExampleParticipant(zkConnectString, clusterName, instanceName, delay);
      process.start();
      System.out.println("Started Instance:" + instanceName);
      Runtime.getRuntime().addShutdownHook(new HelixManagerShutdownHook(process.getManager()));
    }

    Thread.currentThread().join();
  }
}
