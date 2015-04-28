package org.apache.helix.mock.participant;

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
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class DummyProcess {
  private static final Logger logger = Logger.getLogger(DummyProcess.class);
  public static final String zkServer = "zkSvr";
  public static final String cluster = "cluster";
  public static final String hostAddress = "host";
  public static final String hostPort = "port";
  public static final String relayCluster = "relayCluster";
  public static final String help = "help";
  public static final String transDelay = "transDelay";
  public static final String helixManagerType = "helixManagerType";

  private final String _zkConnectString;
  private final String _clusterName;
  private final String _instanceName;
  private DummyStateModelFactory stateModelFactory;

  private int _transDelayInMs = 0;
  private final String _clusterMangerType;

  public DummyProcess(String zkConnectString, String clusterName, String instanceName,
      String clusterMangerType, int delay) {
    _zkConnectString = zkConnectString;
    _clusterName = clusterName;
    _instanceName = instanceName;
    _clusterMangerType = clusterMangerType;
    _transDelayInMs = delay > 0 ? delay : 0;
  }

  static void sleep(long transDelay) {
    try {
      if (transDelay > 0) {
        Thread.sleep(transDelay);
      }
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public HelixManager start() throws Exception {
    HelixManager manager = null;
    // zk cluster manager
    if (_clusterMangerType.equalsIgnoreCase("zk")) {
      manager =
          HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
              InstanceType.PARTICIPANT, _zkConnectString);
    } else {
      throw new IllegalArgumentException("Unsupported cluster manager type:" + _clusterMangerType);
    }

    stateModelFactory = new DummyStateModelFactory(_transDelayInMs);
    DummyLeaderStandbyStateModelFactory stateModelFactory1 =
        new DummyLeaderStandbyStateModelFactory(_transDelayInMs);
    DummyOnlineOfflineStateModelFactory stateModelFactory2 =
        new DummyOnlineOfflineStateModelFactory(_transDelayInMs);
    StateMachineEngine stateMach = manager.getStateMachineEngine();
    stateMach.registerStateModelFactory(StateModelDefId.MasterSlave, stateModelFactory);
    stateMach.registerStateModelFactory(StateModelDefId.LeaderStandby, stateModelFactory1);
    stateMach.registerStateModelFactory(StateModelDefId.OnlineOffline, stateModelFactory2);

    manager.connect();
    return manager;
  }

  public static class DummyStateModelFactory extends StateTransitionHandlerFactory<DummyStateModel> {
    int _delay;

    public DummyStateModelFactory(int delay) {
      _delay = delay;
    }

    @Override
    public DummyStateModel createStateTransitionHandler(ResourceId resource, PartitionId partition) {
      DummyStateModel model = new DummyStateModel();
      model.setDelay(_delay);
      return model;
    }
  }

  public static class DummyLeaderStandbyStateModelFactory extends
      StateTransitionHandlerFactory<DummyLeaderStandbyStateModel> {
    int _delay;

    public DummyLeaderStandbyStateModelFactory(int delay) {
      _delay = delay;
    }

    @Override
    public DummyLeaderStandbyStateModel createStateTransitionHandler(ResourceId resource, PartitionId partition) {
      DummyLeaderStandbyStateModel model = new DummyLeaderStandbyStateModel();
      model.setDelay(_delay);
      return model;
    }
  }

  public static class DummyOnlineOfflineStateModelFactory extends
      StateTransitionHandlerFactory<DummyOnlineOfflineStateModel> {
    int _delay;

    public DummyOnlineOfflineStateModelFactory(int delay) {
      _delay = delay;
    }

    @Override
    public DummyOnlineOfflineStateModel createStateTransitionHandler(ResourceId resource, PartitionId partition) {
      DummyOnlineOfflineStateModel model = new DummyOnlineOfflineStateModel();
      model.setDelay(_delay);
      return model;
    }
  }

  public static class DummyStateModel extends TransitionHandler {
    int _transDelay = 0;

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void onBecomeSlaveFromOffline(Message message, NotificationContext context) {
      PartitionId db = message.getPartitionId();
      String instanceName = context.getManager().getInstanceName();
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeSlaveFromOffline(), instance:" + instanceName + ", db:"
          + db);
    }

    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeSlaveFromMaster()");

    }

    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeMasterFromSlave()");

    }

    public void onBecomeOfflineFromSlave(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeOfflineFromSlave()");

    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeDroppedFromOffline()");

    }
  }

  public static class DummyOnlineOfflineStateModel extends TransitionHandler {
    int _transDelay = 0;

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      PartitionId db = message.getPartitionId();
      String instanceName = context.getManager().getInstanceName();
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeOnlineFromOffline(), instance:" + instanceName + ", db:"
          + db);
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeOfflineFromOnline()");

    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyStateModel.onBecomeDroppedFromOffline()");

    }
  }

  public static class DummyLeaderStandbyStateModel extends TransitionHandler {
    int _transDelay = 0;

    public void setDelay(int delay) {
      _transDelay = delay > 0 ? delay : 0;
    }

    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
      PartitionId db = message.getPartitionId();
      String instanceName = context.getManager().getInstanceName();
      DummyProcess.sleep(_transDelay);
      logger.info("DummyLeaderStandbyStateModel.onBecomeLeaderFromStandby(), instance:"
          + instanceName + ", db:" + db);
    }

    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyLeaderStandbyStateModel.onBecomeStandbyFromLeader()");

    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyLeaderStandbyStateModel.onBecomeDroppedFromOffline()");

    }

    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyLeaderStandbyStateModel.onBecomeStandbyFromOffline()");

    }

    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
      DummyProcess.sleep(_transDelay);

      logger.info("DummyLeaderStandbyStateModel.onBecomeOfflineFromStandby()");

    }
  }

  // TODO hack OptionBuilder is not thread safe
  @SuppressWarnings("static-access")
  synchronized private static Options constructCommandLineOptions() {
    Option helpOption =
        OptionBuilder.withLongOpt(help).withDescription("Prints command-line options info")
            .create();

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

    Option cmTypeOption =
        OptionBuilder
            .withLongOpt(helixManagerType)
            .withDescription(
                "Provide cluster manager type (e.g. 'zk', 'static-file', or 'dynamic-file'")
            .create();
    cmTypeOption.setArgs(1);
    cmTypeOption.setRequired(true);
    cmTypeOption
        .setArgName("Clsuter manager type (e.g. 'zk', 'static-file', or 'dynamic-file') (Required)");

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServer).withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required for zk-based cluster manager)");

    // Option rootNsOption = OptionBuilder.withLongOpt(rootNamespace)
    // .withDescription("Provide root namespace for dynamic-file based cluster manager").create();
    // rootNsOption.setArgs(1);
    // rootNsOption.setRequired(true);
    // rootNsOption.setArgName("Root namespace (Required for dynamic-file based cluster manager)");

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
    options.addOption(hostOption);
    options.addOption(portOption);
    options.addOption(transDelayOption);
    options.addOption(cmTypeOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  public static void printUsage(Options cliOptions) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + DummyProcess.class.getName(), cliOptions);
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
    String cmType = "zk";
    String zkConnectString = "localhost:2181";
    String clusterName = "testCluster";
    String instanceName = "localhost_8900";
    int delay = 0;

    if (args.length > 0) {
      CommandLine cmd = processCommandLineArgs(args);
      zkConnectString = cmd.getOptionValue(zkServer);
      clusterName = cmd.getOptionValue(cluster);

      String host = cmd.getOptionValue(hostAddress);
      String portString = cmd.getOptionValue(hostPort);
      int port = Integer.parseInt(portString);
      instanceName = host + "_" + port;
      cmType = cmd.getOptionValue(helixManagerType);
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
    logger.info("Dummy process started, instanceName:" + instanceName);

    DummyProcess process =
        new DummyProcess(zkConnectString, clusterName, instanceName, cmType, delay);
    HelixManager manager = process.start();

    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      // ClusterManagerFactory.disconnectManagers(instanceName);
      logger.info("participant:" + instanceName + ", " + Thread.currentThread().getName()
          + " interrupted");
      // if (manager != null)
      // {
      // manager.disconnect();
      // }
    } finally {
      if (manager != null) {
        manager.disconnect();
      }
    }

  }
}
