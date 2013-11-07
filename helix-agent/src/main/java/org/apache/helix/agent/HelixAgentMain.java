package org.apache.helix.agent;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class HelixAgentMain {
  private static Logger LOG = Logger.getLogger(HelixAgentMain.class);

  public static final String zkAddr = "zkSvr";
  public static final String cluster = "cluster";
  public static final String help = "help";
  public static final String instanceName = "instanceName";
  public static final String stateModel = "stateModel";

  // hack: OptionalBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

    Option zkAddrOption =
        OptionBuilder.withLongOpt(zkAddr).hasArgs(1).isRequired(true)
            .withArgName("ZookeeperServerAddress(Required)")
            .withDescription("Provide zookeeper address").create();

    Option clusterOption =
        OptionBuilder.withLongOpt(cluster).hasArgs(1).isRequired(true)
            .withArgName("Cluster name (Required)").withDescription("Provide cluster name")
            .create();

    Option instanceNameOption =
        OptionBuilder.withLongOpt(instanceName).hasArgs(1).isRequired(true)
            .withArgName("Helix agent name (Required)").withDescription("Provide Helix agent name")
            .create();

    Option stateModelOption =
        OptionBuilder.withLongOpt(stateModel).hasArgs(1).isRequired(true)
            .withArgName("State model name (Required)").withDescription("Provide state model name")
            .create();

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkAddrOption);
    options.addOption(clusterOption);
    options.addOption(instanceNameOption);
    options.addOption(stateModelOption);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + HelixAgentMain.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();

    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      LOG.error("fail to parse command-line options. cliArgs: " + Arrays.toString(cliArgs), pe);
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  // NOT working for kill -9, working for kill -2/-15
  static class HelixAgentShutdownHook extends Thread {
    final HelixManager _manager;

    HelixAgentShutdownHook(HelixManager manager) {
      _manager = manager;
    }

    @Override
    public void run() {
      LOG.info("HelixAgentShutdownHook invoked. agent: " + _manager.getInstanceName());
      if (_manager != null && _manager.isConnected())
        _manager.disconnect();
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = processCommandLineArgs(args);
    String zkAddress = cmd.getOptionValue(zkAddr);
    String clusterName = cmd.getOptionValue(cluster);
    String instance = cmd.getOptionValue(instanceName);
    String stateModelName = cmd.getOptionValue(stateModel);

    HelixManager manager =
        new ZKHelixManager(clusterName, instance, InstanceType.PARTICIPANT, zkAddress);

    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(StateModelDefId.from(stateModelName),
        new AgentStateModelFactory());

    Runtime.getRuntime().addShutdownHook(new HelixAgentShutdownHook(manager));

    try {
      manager.connect();
      Thread.currentThread().join();
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      if (manager != null && manager.isConnected()) {
        manager.disconnect();
      }
    }
  }
}
