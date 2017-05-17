package org.apache.helix.rest.server;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelixRestMain {
  private static Logger LOG = LoggerFactory.getLogger(HelixRestServer.class);
  private static final String HELP = "help";
  private static final String ZKSERVERADDRESS = "zkSvr";
  private static final String PORT = "port";
  private static final int DEFAULT_PORT = 8100;
  private static final String URI_PREFIX = "/admin/v2";

  private static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + HelixRestServer.class.getName(), cliOptions);
  }

  private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(HELP).withDescription("Prints command-line options info")
            .create();
    helpOption.setArgs(0);
    helpOption.setRequired(false);
    helpOption.setArgName("print help message");

    Option zkServerOption =
        OptionBuilder.withLongOpt(ZKSERVERADDRESS).withDescription("Provide zookeeper address")
            .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option portOption =
        OptionBuilder.withLongOpt(PORT).withDescription("Provide web service port").create();
    portOption.setArgs(1);
    portOption.setRequired(false);
    portOption.setArgName("web service port, default: " + DEFAULT_PORT);

    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOption(portOption);

    return options;
  }

  public static void processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      LOG.error("RestAdminApplication: failed to parse command-line options: " + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    int port = DEFAULT_PORT;
    String zkAddr;
    if (cmd.hasOption(HELP)) {
      printUsage(cliOptions);
      return;
    } else {
      if (cmd.hasOption(PORT)) {
        port = Integer.parseInt(cmd.getOptionValue(PORT));
      }
      zkAddr = String.valueOf(cmd.getOptionValue(ZKSERVERADDRESS));
    }

    final HelixRestServer restServer = new HelixRestServer(zkAddr, port, URI_PREFIX);

    try {
      restServer.start();
      restServer.join();
    } catch (HelixException ex) {
      LOG.error("Failed to start Helix rest server, " + ex);
    } finally {
      restServer.shutdown();
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    processCommandLineArgs(args);
  }
}
