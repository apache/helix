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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.manager.zk.ZkClient;

/**
 * Dumps the Zookeeper file structure on to Disk
 */
@SuppressWarnings("static-access")
public class ZKDumper {
  private ZkClient client;
  private FilenameFilter filter;
  static Options options;
  private String suffix = "";
  // enable by default
  private boolean removeSuffix = false;

  public String getSuffix() {
    return suffix;
  }

  public void setSuffix(String suffix) {
    this.suffix = suffix;
  }

  public boolean isRemoveSuffix() {
    return removeSuffix;
  }

  public void setRemoveSuffix(boolean removeSuffix) {
    this.removeSuffix = removeSuffix;
  }

  static {
    options = new Options();
    OptionGroup optionGroup = new OptionGroup();

    Option d =
        OptionBuilder.withLongOpt("download").withDescription("Download from ZK to File System")
            .create();
    d.setArgs(0);
    Option dSuffix =
        OptionBuilder.withLongOpt("addSuffix")
            .withDescription("add suffix to every file downloaded from ZK").create();
    dSuffix.setArgs(1);
    dSuffix.setRequired(false);

    Option u =
        OptionBuilder.withLongOpt("upload").withDescription("Upload from File System to ZK")
            .create();
    u.setArgs(0);
    Option uSuffix =
        OptionBuilder.withLongOpt("removeSuffix")
            .withDescription("remove suffix from every file uploaded to ZK").create();
    uSuffix.setArgs(0);
    uSuffix.setRequired(false);

    Option del =
        OptionBuilder.withLongOpt("delete").withDescription("Delete given path from ZK").create();

    optionGroup.setRequired(true);
    optionGroup.addOption(del);
    optionGroup.addOption(u);
    optionGroup.addOption(d);
    options.addOptionGroup(optionGroup);
    options.addOption("zkSvr", true, "Zookeeper address");
    options.addOption("zkpath", true, "Zookeeper path");
    options.addOption("fspath", true, "Path on local Filesystem to dump");
    options.addOption("h", "help", false, "Print this usage information");
    options.addOption("v", "verbose", false, "Print out VERBOSE information");
    options.addOption(dSuffix);
    options.addOption(uSuffix);
  }

  public ZKDumper(String zkAddress) {
    client = new ZkClient(zkAddress, ZkClient.DEFAULT_CONNECTION_TIMEOUT);

    ZkSerializer zkSerializer = new ByteArraySerializer();
    client.setZkSerializer(zkSerializer);
    filter = new FilenameFilter() {

      @Override
      public boolean accept(File dir, String name) {
        return !name.startsWith(".");
      }
    };
  }

  public static void main(String[] args) throws Exception {
    if (args == null || args.length == 0) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("java " + ZKDumper.class.getName(), options);
      System.exit(1);
    }
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);
    cmd.hasOption("zkSvr");
    boolean download = cmd.hasOption("download");
    boolean upload = cmd.hasOption("upload");
    boolean del = cmd.hasOption("delete");
    String zkAddress = cmd.getOptionValue("zkSvr");
    String zkPath = cmd.getOptionValue("zkpath");
    String fsPath = cmd.getOptionValue("fspath");

    ZKDumper zkDump = new ZKDumper(zkAddress);
    if (download) {
      if (cmd.hasOption("addSuffix")) {
        zkDump.suffix = cmd.getOptionValue("addSuffix");
      }
      zkDump.download(zkPath, fsPath + zkPath);
    }
    if (upload) {
      if (cmd.hasOption("removeSuffix")) {
        zkDump.removeSuffix = true;
      }
      zkDump.upload(zkPath, fsPath);
    }
    if (del) {
      zkDump.delete(zkPath);
    }
  }

  private void delete(String zkPath) {
    client.deleteRecursive(zkPath);

  }

  public void upload(String zkPath, String fsPath) throws Exception {
    File file = new File(fsPath);
    System.out.println("Uploading " + file.getCanonicalPath() + " to " + zkPath);
    zkPath = zkPath.replaceAll("[/]+", "/");
    int index = -1;
    if (removeSuffix && (index = file.getName().indexOf(".")) > -1) {
      zkPath = zkPath.replaceAll(file.getName().substring(index), "");
    }
    if (file.isDirectory()) {
      File[] children = file.listFiles(filter);
      client.createPersistent(zkPath, true);
      if (children != null && children.length > 0) {

        for (File child : children) {
          upload(zkPath + "/" + child.getName(), fsPath + "/" + child.getName());
        }
      } else {

      }
    } else {
      byte[] result = new byte[(int) file.length()];
      InputStream input = null;
      try {
        int totalBytesRead = 0;
        input = new BufferedInputStream(new FileInputStream(file));
        while (totalBytesRead < result.length) {
          int bytesRemaining = result.length - totalBytesRead;
          // input.read() returns -1, 0, or more :
          int bytesRead = input.read(result, totalBytesRead, bytesRemaining);
          if (bytesRead > 0) {
            totalBytesRead = totalBytesRead + bytesRead;
          }
        }
        /*
         * the above style is a bit tricky: it places bytes into the 'result'
         * array; 'result' is an output parameter; the while loop usually has a
         * single iteration only.
         */

        client.createPersistent(zkPath, result);
      } finally {
        input.close();
      }

    }
  }

  public void download(String zkPath, String fsPath) throws Exception {

    List<String> children = client.getChildren(zkPath);
    if (children != null && children.size() > 0) {
      new File(fsPath).mkdirs();
      for (String child : children) {
        String childPath = zkPath.equals("/") ? "/" + child : zkPath + "/" + child;
        download(childPath, fsPath + "/" + child);
      }
    } else {
      System.out
          .println("Saving " + zkPath + " to " + new File(fsPath + suffix).getCanonicalPath());
      OutputStream out = new FileOutputStream(fsPath + suffix);
      Object readData = client.readData(zkPath);
      if (readData != null) {
        out.write((byte[]) readData);
      }
      out.close();
    }
  }
}
