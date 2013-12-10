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

import java.util.ArrayList;
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
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;

/**
 * collection of test utilities for integration tests
 */
public class IntegrationTestUtil {
  private static Logger LOG = Logger.getLogger(IntegrationTestUtil.class);

  public static final long defaultTimeout = 30 * 1000; // in milliseconds
  public static final String help = "help";
  public static final String zkSvr = "zkSvr";

  public static final String verifyExternalView = "verifyExternalView";
  public static final String verifyLiveNodes = "verifyLiveNodes";
  public static final String readZNode = "readZNode";
  public static final String readLeader = "readLeader";

  final ZkClient _zkclient;
  final ZNRecordSerializer _serializer;

  public IntegrationTestUtil(ZkClient zkclient) {
    _zkclient = zkclient;
    _serializer = new ZNRecordSerializer();
  }

  public void verifyExternalView(String[] args) {
    if (args == null || args.length == 0) {
      System.err.println("Illegal arguments for " + verifyExternalView);
      return;
    }

    long timeoutValue = defaultTimeout;

    String clusterName = args[0];
    List<String> liveNodes = new ArrayList<String>();
    for (int i = 1; i < args.length; i++) {
      liveNodes.add(args[i]);
    }

    ClusterExternalViewVerifier verifier =
        new ClusterExternalViewVerifier(_zkclient, clusterName, liveNodes);
    boolean success = verifier.verifyByPolling(timeoutValue);
    System.out.println(success ? "Successful" : "Failed");

  }

  public void verifyLiveNodes(String[] args) {
    if (args == null || args.length == 0) {
      System.err.println("Illegal arguments for " + verifyLiveNodes);
      return;
    }

    long timeoutValue = defaultTimeout;

    String clusterName = args[0];
    List<String> liveNodes = new ArrayList<String>();
    for (int i = 1; i < args.length; i++) {
      liveNodes.add(args[i]);
    }

    ClusterLiveNodesVerifier verifier =
        new ClusterLiveNodesVerifier(_zkclient, clusterName, liveNodes);
    boolean success = verifier.verifyByPolling(timeoutValue);
    System.out.println(success ? "Successful" : "Failed");
  }

  public void readZNode(String path) {
    ZNRecord record = _zkclient.readData(path, true);
    if (record == null) {
      System.out.println("null");
    } else {
      System.out.println(new String(_serializer.serialize(record)));
    }
  }

  @SuppressWarnings("static-access")
  static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options information")
            .create();

    Option zkSvrOption =
        OptionBuilder.hasArgs(1).isRequired(true).withArgName("zookeeperAddress")
            .withLongOpt(zkSvr).withDescription("Provide zookeeper-address").create();

    Option verifyExternalViewOption =
        OptionBuilder.hasArgs().isRequired(false).withArgName("clusterName node1 node2..")
            .withLongOpt(verifyExternalView).withDescription("Verify external-view").create();

    Option verifyLiveNodesOption =
        OptionBuilder.hasArg().isRequired(false).withArgName("clusterName node1, node2..")
            .withLongOpt(verifyLiveNodes).withDescription("Verify live-nodes").create();

    Option readZNodeOption =
        OptionBuilder.hasArgs(1).isRequired(false).withArgName("zkPath").withLongOpt(readZNode)
            .withDescription("Read znode").create();

    Option readLeaderOption =
        OptionBuilder.hasArgs(1).isRequired(false).withArgName("clusterName")
            .withLongOpt(readLeader).withDescription("Read cluster controller").create();

    OptionGroup optGroup = new OptionGroup();
    optGroup.setRequired(true);
    optGroup.addOption(verifyExternalViewOption);
    optGroup.addOption(verifyLiveNodesOption);
    optGroup.addOption(readZNodeOption);
    optGroup.addOption(readLeaderOption);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkSvrOption);
    options.addOptionGroup(optGroup);

    return options;
  }

  static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + IntegrationTestUtil.class.getName(), cliOptions);
  }

  static void processCommandLineArgs(String[] cliArgs) {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;
    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("failed to parse command-line args: " + Arrays.asList(cliArgs)
          + ", exception: " + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    String zkServer = cmd.getOptionValue(zkSvr);

    ZkClient zkclient =
        new ZkClient(zkServer, ZkClient.DEFAULT_SESSION_TIMEOUT,
            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    IntegrationTestUtil util = new IntegrationTestUtil(zkclient);

    if (cmd != null) {
      if (cmd.hasOption(verifyExternalView)) {
        String[] args = cmd.getOptionValues(verifyExternalView);
        util.verifyExternalView(args);
      } else if (cmd.hasOption(verifyLiveNodes)) {
        String[] args = cmd.getOptionValues(verifyLiveNodes);
        util.verifyLiveNodes(args);
      } else if (cmd.hasOption(readZNode)) {
        String path = cmd.getOptionValue(readZNode);
        util.readZNode(path);
      } else if (cmd.hasOption(readLeader)) {
        String clusterName = cmd.getOptionValue(readLeader);
        PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
        util.readZNode(keyBuilder.controllerLeader().getPath());
      } else {
        printUsage(cliOptions);
      }
    }
  }

  public static void main(String[] args) {
    processCommandLineArgs(args);
  }
}
