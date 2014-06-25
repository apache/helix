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

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

/**
 * Tool for copying a zk/file path to another zk/file path
 */
public class ZkCopy {
  enum ZkCopyScheme {
    zk
  }

  private static Logger logger = Logger.getLogger(ZkCopy.class);
  private static final String src = "src";
  private static final String dst = "dst";

  @SuppressWarnings("static-access")
  private static Options constructCmdLineOpt() {
    Option srcOpt =
        OptionBuilder.withLongOpt(src).hasArgs(1).isRequired(true)
            .withArgName("source-URI (e.g. zk://localhost:2181/src-path")
            .withDescription("Provide source URI").create();

    Option dstOpt =
        OptionBuilder.withLongOpt(dst).hasArgs(1).isRequired(true)
            .withArgName("destination-URI (e.g. zk://localhost:2181/dst-path")
            .withDescription("Provide destination URI").create();

    Options options = new Options();
    options.addOption(srcOpt);
    options.addOption(dstOpt);
    return options;
  }

  private static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ZkCopy.class.getName(), cliOptions);
  }

  private static String concatenate(String path, String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return path;
    }

    if (path.endsWith("/") || suffix.startsWith("/")) {
      return path + suffix;
    } else {
      return path + "/" + suffix;
    }
  }

  private static void zkCopy(ZkClient srcClient, String srcPath, ZkClient dstClient, String dstPath) {
    // Strip off tailing "/"
    if (!srcPath.equals("/") && srcPath.endsWith("/")) {
      srcPath = srcPath.substring(0, srcPath.length() - 1);
    }

    if (!dstPath.equals("/") && dstPath.endsWith("/")) {
      dstPath = dstPath.substring(0, dstPath.length() - 1);
    }

    // Validate paths
    PathUtils.validatePath(srcPath);
    PathUtils.validatePath(dstPath);

    if (srcPath.equals(dstPath)) {
      logger.info("srcPath == dstPath. Skip copying");
      return;
    }

    if (srcPath.startsWith(dstPath) || dstPath.startsWith(srcPath)) {
      throw new IllegalArgumentException(
          "srcPath/dstPath can't be prefix of dstPath/srcPath, was srcPath: " + srcPath
              + ", dstPath: " + dstPath);
    }

    // Recursive copy using BFS
    List<String> queue = new LinkedList<String>();
    queue.add("");
    while (!queue.isEmpty()) {
      String path = queue.remove(0);
      Stat stat = new Stat();
      String fromPath = concatenate(srcPath, path);
      Object data = srcClient.readDataAndStat(fromPath, stat, false);
      if (stat.getEphemeralOwner() != 0) {
        logger.info("Skip copying ephemeral znode: " + fromPath);
        continue;
      }
      String toPath = concatenate(dstPath, path);
      System.out.println("Copy " + fromPath + " to " + toPath);
      dstClient.createPersistent(toPath, data);
      List<String> children = srcClient.getChildren(fromPath);
      if (children != null && children.size() > 0) {
        for (String child : children) {
          queue.add(concatenate(path, child));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCmdLineOpt();
    CommandLine cmd = null;

    try {
      cmd = cliParser.parse(cliOptions, args);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    URI srcUri = new URI(cmd.getOptionValue(src));
    URI dstUri = new URI(cmd.getOptionValue(dst));

    ZkCopyScheme srcScheme = ZkCopyScheme.valueOf(srcUri.getScheme());
    ZkCopyScheme dstScheme = ZkCopyScheme.valueOf(dstUri.getScheme());

    if (srcScheme == ZkCopyScheme.zk && dstScheme == ZkCopyScheme.zk) {
      String srcZkAddr = srcUri.getAuthority();
      String dstZkAddr = dstUri.getAuthority();

      ZkClient srcClient = null;
      ZkClient dstClient = null;
      try {
        if (srcZkAddr.equals(dstZkAddr)) {
          srcClient =
              dstClient =
                  new ZkClient(srcZkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
                      ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ByteArraySerializer());
        } else {
          srcClient =
              new ZkClient(srcZkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
                  ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ByteArraySerializer());
          dstClient =
              new ZkClient(dstZkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
                  ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ByteArraySerializer());
        }
        String srcPath = srcUri.getPath();
        String dstPath = dstUri.getPath();
        zkCopy(srcClient, srcPath, dstClient, dstPath);
      } finally {
        if (srcClient != null) {
          srcClient.close();
        }
        if (dstClient != null) {
          dstClient.close();
        }
      }
    } else {
      System.err.println("Unsupported scheme. srcScheme: " + srcScheme + ", dstScheme: " + dstScheme);
      System.exit(1);
    }
  }
}
