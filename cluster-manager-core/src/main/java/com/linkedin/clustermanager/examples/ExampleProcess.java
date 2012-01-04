package com.linkedin.clustermanager.examples;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerFactory;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

public class ExampleProcess
{

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
  private final String stateModelType;
  private ClusterManager manager;

  private StateMachineEngine genericStateMachineHandler;

  private String _file = null;
  private StateModelFactory<StateModel> stateModelFactory;
  private final int delay;

  public ExampleProcess(String zkConnectString, String clusterName,
      String instanceName, String file, String stateModel, int delay)
  {
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.instanceName = instanceName;
    this._file = file;
    stateModelType = stateModel;
    this.delay = delay;
  }

  public void start() throws Exception
  {
    if (_file == null)
      manager = ClusterManagerFactory.getZKBasedManagerForParticipant(
          clusterName, instanceName, zkConnectString);
    else
      manager = ClusterManagerFactory.getFileBasedManagerForParticipant(
          clusterName, instanceName, _file);
    if ("MasterSlave".equalsIgnoreCase(stateModelType))
    {
      stateModelFactory = new MasterSlaveStateModelFactory(delay);
    } else if ("OnlineOffline".equalsIgnoreCase(stateModelType))
    {
      stateModelFactory = new OnlineOfflineStateModelFactory(delay);
    } else if ("LeaderStandby".equalsIgnoreCase(stateModelType))
    {
      stateModelFactory = new LeaderStandbyStateModelFactory(delay);
    }
    genericStateMachineHandler = new StateMachineEngine();
    genericStateMachineHandler.registerStateModelFactory(stateModelType, stateModelFactory);
    manager.connect();
    manager.getMessagingService().registerMessageHandlerFactory(
        MessageType.STATE_TRANSITION.toString(), genericStateMachineHandler);
    if (_file != null)
    {
      ClusterStateVerifier.verifyFileBasedClusterStates(_file, instanceName,
          stateModelFactory);

    }
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option zkServerOption = OptionBuilder.withLongOpt(zkServer)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option clusterOption = OptionBuilder.withLongOpt(cluster)
        .withDescription("Provide cluster name").create();
    clusterOption.setArgs(1);
    clusterOption.setRequired(true);
    clusterOption.setArgName("Cluster name (Required)");

    Option hostOption = OptionBuilder.withLongOpt(hostAddress)
        .withDescription("Provide host name").create();
    hostOption.setArgs(1);
    hostOption.setRequired(true);
    hostOption.setArgName("Host name (Required)");

    Option portOption = OptionBuilder.withLongOpt(hostPort)
        .withDescription("Provide host port").create();
    portOption.setArgs(1);
    portOption.setRequired(true);
    portOption.setArgName("Host port (Required)");

    Option stateModelOption = OptionBuilder.withLongOpt(stateModel)
        .withDescription("StateModel Type").create();
    stateModelOption.setArgs(1);
    stateModelOption.setRequired(true);
    stateModelOption.setArgName("StateModel Type (Required)");

    // add an option group including either --zkSvr or --configFile
    Option fileOption = OptionBuilder.withLongOpt(configFile)
        .withDescription("Provide file to read states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to read states/messages (Optional)");

    Option transDelayOption = OptionBuilder.withLongOpt(transDelay)
        .withDescription("Provide state trans delay").create();
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

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ExampleProcess.class.getName(), cliOptions);
  }

  public static CommandLine processCommandLineArgs(String[] cliArgs)
      throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    try
    {
      return cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }
    return null;
  }

  public static void main(String[] args) throws Exception
  {
    String zkConnectString = "localhost:2181";
    String clusterName = "storage-integration-cluster";
    String instanceName = "localhost_8905";
    String file = null;
    String stateModelValue = "MasterSlave";
    int delay = 0;
    boolean skipZeroArgs = true;// false is for dev testing
    if (!skipZeroArgs || args.length > 0)
    {
      CommandLine cmd = processCommandLineArgs(args);
      zkConnectString = cmd.getOptionValue(zkServer);
      clusterName = cmd.getOptionValue(cluster);

      String host = cmd.getOptionValue(hostAddress);
      String portString = cmd.getOptionValue(hostPort);
      int port = Integer.parseInt(portString);
      instanceName = host + "_" + port;

      file = cmd.getOptionValue(configFile);
      if (file != null)
      {
        File f = new File(file);
        if (!f.exists())
        {
          System.err.println("static config file doesn't exist");
          System.exit(1);
        }
      }

      stateModelValue = cmd.getOptionValue(stateModel);
      if (cmd.hasOption(transDelay))
      {
        try
        {
          delay = Integer.parseInt(cmd.getOptionValue(transDelay));
          if (delay < 0)
          {
            throw new Exception("delay must be positive");
          }
        } catch (Exception e)
        {
          e.printStackTrace();
          delay = 0;
        }
      }
    }
    // Espresso_driver.py will consume this
    System.out.println("Starting Process with ZK:" + zkConnectString);

    ExampleProcess process = new ExampleProcess(zkConnectString, clusterName,
        instanceName, file, stateModelValue, delay);

    process.start();
    Thread.currentThread().join();
  }
}
