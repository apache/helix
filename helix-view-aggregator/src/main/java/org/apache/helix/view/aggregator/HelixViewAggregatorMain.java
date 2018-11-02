package org.apache.helix.view.aggregator;

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

import java.util.logging.Level;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixViewAggregatorMain {
  private static Logger logger = LoggerFactory.getLogger(HelixViewAggregatorMain.class);
  private static final String HELP = "help";
  private static final String ZK_ADDR = "zookeeper-address";
  private static final String VIEW_CLUSTER_NAME = "view-cluster-name";

  private static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + HelixViewAggregatorMain.class.getName(), cliOptions);
  }

  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(HELP).withDescription("Prints command-line options info")
            .create();
    helpOption.setArgs(0);
    helpOption.setRequired(false);
    helpOption.setArgName("print help message");

    Option zkServerOption =
        OptionBuilder.withLongOpt(ZK_ADDR).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZooKeeper server connection string (Required)");

    Option portOption =
        OptionBuilder.withLongOpt(VIEW_CLUSTER_NAME).withDescription("Name of the view cluster")
            .create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("Name of the view cluster");

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(portOption);

    return options;
  }
  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.INFO);
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, args);
    } catch (ParseException pe) {
      logger.error("HelixViewAggregatorMain: failed to parse command-line options.", pe);
      printUsage(cliOptions);
      System.exit(1);
    }

    String zkAddr, viewClusterName;
    if (cmd.hasOption(HELP)) {
      printUsage(cliOptions);
      return;
    } else {
      zkAddr = String.valueOf(cmd.getOptionValue(ZK_ADDR));
      viewClusterName = String.valueOf(cmd.getOptionValue(VIEW_CLUSTER_NAME));
    }

    final HelixViewAggregator aggregator = new HelixViewAggregator(viewClusterName, zkAddr);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        aggregator.shutdown();
      }
    }));

    try {
      aggregator.start();
      Thread.currentThread().join();
    } catch (Exception e) {
      logger.error("HelixViewAggregator caught exception.", e);
    } finally {
      aggregator.shutdown();
    }

    // Service should not exit successfully
    System.exit(1);
  }
}
