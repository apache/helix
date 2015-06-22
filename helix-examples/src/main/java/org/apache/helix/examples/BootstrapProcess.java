package org.apache.helix.examples;

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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

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
import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.participant.StateMachineEngine;

/**
 * This process does little more than handling the state transition messages.
 * This is generally the case when the server needs to bootstrap when it comes
 * up.<br>
 * Flow for a typical Master-slave state model<br>
 * <ul>
 * <li>Gets OFFLINE-SLAVE transition</li>
 * <li>Figure out if it has any data and how old it is for the SLAVE partition</li>
 * <li>If the data is fresh enough it can probably catch up from the replication stream of the
 * master</li>
 * <li>If not, then it can use the messaging service provided by cluster manager to talk other nodes
 * to figure out if they have any backup</li> </li>
 * <li>Once it gets a response from other nodes in the cluster the process can decide which back up
 * it wants to use to bootstrap</li>
 * </ul>
 */
public class BootstrapProcess {
  static final String REQUEST_BOOTSTRAP_URL = "REQUEST_BOOTSTRAP_URL";
  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String hostAddress = "host";
  public static final String hostPort = "port";
  public static final String relayCluster = "relayCluster";
  public static final String help = "help";
  public static final String configFile = "configFile";
  public static final String stateModel = "stateModelType";
  public static final String transDelay = "transDelay";

  private final String zkConnectString;
  private final String clusterName;
  private final String instanceName;
  private HelixManager manager;

  private StateTransitionHandlerFactory<TransitionHandler> stateModelFactory;

  public BootstrapProcess(String zkConnectString, String clusterName, String instanceName,
      String stateModel, int delay) {
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
  }

  public void start() throws Exception {
    manager =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkConnectString);

    stateModelFactory = new BootstrapHandler();

    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(StateModelDefId.MasterSlave, stateModelFactory);

    manager.getMessagingService().registerMessageHandlerFactory(
        MessageType.STATE_TRANSITION.toString(), stateMach);
    manager.getMessagingService().registerMessageHandlerFactory(
        MessageType.USER_DEFINE_MSG.toString(), new CustomMessageHandlerFactory());
    manager.connect();
  }

  public static class CustomMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {

      return new CustomMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return MessageType.USER_DEFINE_MSG.toString();
    }

    @Override
    public void reset() {

    }

    static class CustomMessageHandler extends MessageHandler {

      public CustomMessageHandler(Message message, NotificationContext context) {
        super(message, context);
        // TODO Auto-generated constructor stub
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        String hostName;
        HelixTaskResult result = new HelixTaskResult();
        try {
          hostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
          hostName = "UNKNOWN";
        }
        String port = "2134";
        String msgSubType = _message.getMsgSubType();
        if (msgSubType.equals(REQUEST_BOOTSTRAP_URL)) {
          result.getTaskResultMap().put(
              "BOOTSTRAP_URL",
              "http://" + hostName + ":" + port + "/getFile?path=/data/bootstrap/"
                  + _message.getResourceId().stringify() + "/"
                  + _message.getPartitionId().stringify() + ".tar");

          result.getTaskResultMap().put("BOOTSTRAP_TIME", "" + new Date().getTime());
        }

        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        e.printStackTrace();
      }
    }
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

    Option hostOption =
        OptionBuilder.withLongOpt(hostAddress).withDescription("Provide host name").create();
    hostOption.setArgs(1);
    hostOption.setRequired(true);
    hostOption.setArgName("Host name (Required)");

    Option portOption =
        OptionBuilder.withLongOpt(hostPort).withDescription("Provide host port").create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("Host port (Required)");

    Option stateModelOption =
        OptionBuilder.withLongOpt(stateModel).withDescription("StateModel Type").create();
    stateModelOption.setArgs(1);
    stateModelOption.setRequired(true);
    stateModelOption.setArgName("StateModel Type (Required)");

    // add an option group including either --zkSvr or --configFile
    Option fileOption =
        OptionBuilder.withLongOpt(configFile)
            .withDescription("Provide file to read states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to read states/messages (Optional)");

    Option transDelayOption =
        OptionBuilder.withLongOpt(transDelay).withDescription("Provide state trans delay").create();
    transDelayOption.setArgs(1);
    transDelayOption.setRequired(false);
    transDelayOption.setArgName("Delay time in state transition, in MS");

    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);
    optionGroup.addOption(fileOption);

    Options options = new Options();
    options.addOption(helpOption);
    // options.addOption(zkServerOption);
    options.addOption(clusterOption);
    options.addOption(hostOption);
    options.addOption(portOption);
    options.addOption(stateModelOption);
    options.addOption(transDelayOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + BootstrapProcess.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs) throws Exception {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    try {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe) {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    String zkConnectString = "localhost:2181";
    String clusterName = "storage-integration-cluster";
    String instanceName = "localhost_8905";
    String stateModelValue = "MasterSlave";
    int delay = 0;
    boolean skipZeroArgs = true;// false is for dev testing
    if (!skipZeroArgs || args.length > 0) {
      CommandLine cmd = processCommandLineArgs(args);
      zkConnectString = cmd.getOptionValue(zkServer);
      clusterName = cmd.getOptionValue(cluster);

      String host = cmd.getOptionValue(hostAddress);
      String portString = cmd.getOptionValue(hostPort);
      int port = Integer.parseInt(portString);
      instanceName = host + "_" + port;

      stateModelValue = cmd.getOptionValue(stateModel);
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
    }
    // Espresso_driver.py will consume this
    System.out.println("Starting Process with ZK:" + zkConnectString);

    BootstrapProcess process =
        new BootstrapProcess(zkConnectString, clusterName, instanceName, stateModelValue, delay);

    process.start();
    Thread.currentThread().join();
  }
}

class BootstrapReplyHandler extends AsyncCallback {

  public BootstrapReplyHandler() {
  }

  private String bootstrapUrl;
  private String bootstrapTime;

  @Override
  public void onTimeOut() {
    System.out.println("Timed out");
  }

  public String getBootstrapUrl() {
    return bootstrapUrl;
  }

  public String getBootstrapTime() {
    return bootstrapTime;
  }

  @Override
  public void onReplyMessage(Message message) {
    String time = message.getResultMap().get("BOOTSTRAP_TIME");
    if (bootstrapTime == null || time.compareTo(bootstrapTime) > -1) {
      bootstrapTime = message.getResultMap().get("BOOTSTRAP_TIME");
      bootstrapUrl = message.getResultMap().get("BOOTSTRAP_URL");
    }
  }

}
