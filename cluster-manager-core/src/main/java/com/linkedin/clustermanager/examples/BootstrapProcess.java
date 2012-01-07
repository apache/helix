package com.linkedin.clustermanager.examples;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

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
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.Criteria;
import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.messaging.AsyncCallback;
import com.linkedin.clustermanager.messaging.handling.CMTaskExecutor;
import com.linkedin.clustermanager.messaging.handling.MessageHandler;
import com.linkedin.clustermanager.messaging.handling.MessageHandlerFactory;
import com.linkedin.clustermanager.model.Message;
import com.linkedin.clustermanager.model.Message.MessageType;
import com.linkedin.clustermanager.participant.StateMachineEngine;
import com.linkedin.clustermanager.participant.statemachine.StateModel;
import com.linkedin.clustermanager.participant.statemachine.StateModelFactory;
import com.linkedin.clustermanager.participant.statemachine.StateModelInfo;
import com.linkedin.clustermanager.participant.statemachine.Transition;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.tools.ClusterStateVerifier;

/**
 * This process does little more than handling the state transition messages.
 * This is generally the case when the server needs to bootstrap when it comes
 * up.<br>
 * Flow for a typical Master-slave state model<br>
 * <ul>
 * <li>Gets OFFLINE-SLAVE transition</li>
 * <li>Figure out if it has any data and how old it is for the SLAVE partition</li>
 * <li>If the data is fresh enough it can probably catch up from the replication
 * stream of the master</li>
 * <li>If not, then it can use the messaging service provided by cluster manager
 * to talk other nodes to figure out if they have any backup</li>
 * </li>
 * <li>Once it gets a response from other nodes in the cluster the process can
 * decide which back up it wants to use to bootstrap</li>
 * </ul>
 * 
 * @author kgopalak
 * 
 */
public class BootstrapProcess
{
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
  private final String stateModelType;
  private ClusterManager manager;

  private StateMachineEngine genericStateMachineHandler;

  private String _file = null;
  private StateModelFactory<StateModel> stateModelFactory;
  private final int delay;

  public BootstrapProcess(String zkConnectString, String clusterName,
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
    stateModelFactory = new BootstrapHandler();
    genericStateMachineHandler = new StateMachineEngine();
    genericStateMachineHandler.registerStateModelFactory("MasterSlave", stateModelFactory);
    manager.getMessagingService().registerMessageHandlerFactory(
        MessageType.STATE_TRANSITION.toString(), genericStateMachineHandler);
    manager.getMessagingService().registerMessageHandlerFactory(
        MessageType.USER_DEFINE_MSG.toString(),
        new CustomMessageHandlerFactory());
    manager.connect();
    if (_file != null)
    {
      ClusterStateVerifier.verifyFileBasedClusterStates(_file, instanceName,
          stateModelFactory);

    }
  }

  public static class CustomMessageHandlerFactory implements
      MessageHandlerFactory
  {

    @Override
    public MessageHandler createHandler(Message message,
        NotificationContext context)
    {
      
      return new CustomMessageHandler();
    }

    @Override
    public String getMessageType()
    {
      return MessageType.USER_DEFINE_MSG.toString();
    }

    @Override
    public void reset()
    {

    }

    static class CustomMessageHandler implements MessageHandler
    {

      @Override
      public void handleMessage(Message message, NotificationContext context,
          Map<String, String> resultMap) throws InterruptedException
      {
        String hostName;
        try
        {
          hostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e)
        {
          hostName = "UNKNOWN";
        }
        String port = "2134";
        String msgSubType = message.getMsgSubType();
        if (msgSubType.equals(REQUEST_BOOTSTRAP_URL))
        {
          resultMap.put(
              "BOOTSTRAP_URL",
              "http://" + hostName + ":" + port
                  + "/getFile?path=/data/bootstrap/"
                  + message.getResourceGroupName() + "/"
                  + message.getResourceKey() + ".tar");
          
          resultMap.put(
              "BOOTSTRAP_TIME",
              ""+new Date().getTime());
        }
      }
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
    helpFormatter.printHelp("java " + BootstrapProcess.class.getName(), cliOptions);
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

    BootstrapProcess process = new BootstrapProcess(zkConnectString,
        clusterName, instanceName, file, stateModelValue, delay);

    process.start();
    Thread.currentThread().join();
  }
}

class BootstrapReplyHandler extends AsyncCallback
{

  public BootstrapReplyHandler()
  {
  }

  private String bootstrapUrl;
  private String bootstrapTime;

  @Override
  public void onTimeOut()
  {
    System.out.println("Timed out");
  }

  public String getBootstrapUrl()
  {
    return bootstrapUrl;
  }

  public String getBootstrapTime()
  {
    return bootstrapTime;
  }

  @Override
  public void onReplyMessage(Message message)
  {
    String time = message.getResultMap().get("BOOTSTRAP_TIME");
    if (bootstrapTime == null || time.compareTo(bootstrapTime) > -1)
    {
      bootstrapTime = message.getResultMap().get("BOOTSTRAP_TIME");
      bootstrapUrl = message.getResultMap().get("BOOTSTRAP_URL");
    }
  }

}
