package com.linkedin.clustermanager.tools;

import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.clustermanager.controller.IdealStateCalculatorByShuffling;
import com.linkedin.clustermanager.core.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.core.ClusterManagementTool;
import com.linkedin.clustermanager.core.listeners.ClusterManagerException;
import com.linkedin.clustermanager.impl.file.FileBasedClusterManager;
import com.linkedin.clustermanager.impl.zk.ZKClusterManagementTool;
import com.linkedin.clustermanager.impl.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.model.ClusterView;
import com.linkedin.clustermanager.model.ZNRecord;

public class ClusterSetup
{
  public static final String zkServerAddress = "zkSvr";

  // List info about the cluster / DB/ Nodes
  public static final String listClusters = "listClusters";
  public static final String listDatabase = "listDatabase";
  public static final String listNodes = "listNodes";

  // Add and rebalance
  public static final String addCluster = "addCluster";
  public static final String addNode = "addNode";
  public static final String addDatabase = "addDatabase";
  public static final String rebalance = "rebalance";

  // Query info (TBD in V2)
  public static final String clusterInfo = "clusterInfo";
  public static final String nodeInfo = "nodeInfo";
  public static final String databaseInfo = "databaseInfo";
  public static final String partitionInfo = "partitionInfo";

  // setup for file-based cluster manager
  public static final String configFile = "configFile";

  // enable / disable nodes
  public static final String enableNode = "enableNode";

  public static final String help = "help";

  static Logger _logger = Logger.getLogger(ClusterSetup.class);
  String _zkServerAddress;

  public ClusterSetup(String zkServerAddress)
  {
    _zkServerAddress = zkServerAddress;
  }

  public void addCluster(String clusterName, boolean overwritePrevious)
  {
    ClusterManagementTool managementTool = getClusterManagementTool();
    managementTool.addCluster(clusterName, overwritePrevious);
  }

  public void addNodesToCluster(String clusterName, String[] nodeInfoArray)
  {
    for (String nodeInfo : nodeInfoArray)
    {
      // the storage node info must be hostname:port format.
      if (nodeInfo.length() > 0)
      {
        addNodeToCluster(clusterName, nodeInfo);
      }
    }
  }

  public void addNodeToCluster(String clusterName, String nodeAddress)
  {
    // nodeAddress must be in host:port format
    int lastPos = nodeAddress.lastIndexOf(":");
    if (lastPos <= 0)
    {
      String error = "Invalid storage node info format: " + nodeAddress;
      _logger.warn(error);
      throw new ClusterManagerException(error);
    }
    String host = nodeAddress.substring(0, lastPos);
    String portStr = nodeAddress.substring(lastPos + 1);
    int port = Integer.parseInt(portStr);
    addNodeToCluster(clusterName, host, port);
  }

  public void addNodeToCluster(String clusterName, String host, int port)
  {
    ClusterManagementTool managementTool = getClusterManagementTool();

    ZNRecord nodeConfig = new ZNRecord();
    String nodeId = host + "_" + port;
    nodeConfig.setId(nodeId);
    nodeConfig.setSimpleField(InstanceConfigProperty.HOST.toString(), host);
    nodeConfig.setSimpleField(InstanceConfigProperty.PORT.toString(), "" + port);
    nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(), true + "");

    managementTool.addNode(clusterName, nodeConfig);
  }

  public ClusterManagementTool getClusterManagementTool()
  {
    ZkClient zkClient = new ZkClient(_zkServerAddress);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    return new ZKClusterManagementTool(zkClient);
  }

  public void addDatabaseToCluster(String clusterName, String dbName, int partitions)
  {
    ClusterManagementTool managementTool = getClusterManagementTool();
    managementTool.addDatabase(clusterName, dbName, partitions);
  }

  public void rebalanceStorageCluster(String clusterName, String dbName, int replica)
  {
    ClusterManagementTool managementTool = getClusterManagementTool();
    List<String> nodeNames = managementTool.getNodeNamesInCluster(clusterName);

    ZNRecord dbIdealState = managementTool.getDBIdealState(clusterName, dbName);
    int partitions = Integer.parseInt(dbIdealState.getSimpleField("partitions"));

    ZNRecord idealState = IdealStateCalculatorByShuffling.calculateIdealState(nodeNames,
        partitions, replica, dbName);
    managementTool.setDBIdealState(clusterName, dbName, idealState);
  }

  public void setupTestCluster(String clusterName)
  {
    addCluster(clusterName, true);
    String storageNodeInfoArray[] = new String[6];
    for (int i = 0; i < storageNodeInfoArray.length; i++)
    {
      storageNodeInfoArray[i] = "localhost:" + (8900 + i);
    }

    addNodesToCluster(clusterName, storageNodeInfoArray);
    addDatabaseToCluster(clusterName, "EspressoDB", 10);
    rebalanceStorageCluster(clusterName, "EspressoDB", 3);
  }

  public static void printUsage(Options cliOptions)
  {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption = OptionBuilder.withLongOpt(help)
        .withDescription("Prints command-line options info").create();

    Option zkServerOption = OptionBuilder.withLongOpt(zkServerAddress)
        .withDescription("Provide zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");

    Option listClustersOption = OptionBuilder.withLongOpt(listClusters)
        .withDescription("List existing clusters").create();
    listClustersOption.setArgs(0);
    listClustersOption.setRequired(false);

    Option listDatabaseOption = OptionBuilder.withLongOpt(listDatabase)
        .withDescription("List databases hosted in a cluster").create();
    listDatabaseOption.setArgs(1);
    listDatabaseOption.setRequired(false);
    listDatabaseOption.setArgName("clusterName");

    Option listNodesOption = OptionBuilder.withLongOpt(listNodes)
        .withDescription("List nodes in a cluster").create();
    listNodesOption.setArgs(1);
    listNodesOption.setRequired(false);
    listNodesOption.setArgName("clusterName");

    Option addClusterOption = OptionBuilder.withLongOpt(addCluster)
        .withDescription("Add a new cluster").create();
    addClusterOption.setArgs(1);
    addClusterOption.setRequired(false);
    addClusterOption.setArgName("clusterName");

    Option addNodeOption = OptionBuilder.withLongOpt(addNode)
        .withDescription("Add a new node to a cluster").create();
    addNodeOption.setArgs(2);
    addNodeOption.setRequired(false);
    addNodeOption.setArgName("clusterName nodeAddress(host:port)");

    Option addDatabaseOption = OptionBuilder.withLongOpt(addDatabase)
        .withDescription("Add a database to a cluster").create();
    addDatabaseOption.setArgs(3);
    addDatabaseOption.setRequired(false);
    addDatabaseOption.setArgName("clusterName dbName partitionNo");

    Option rebalanceOption = OptionBuilder.withLongOpt(rebalance)
        .withDescription("Rebalance a database in a cluster").create();
    rebalanceOption.setArgs(3);
    rebalanceOption.setRequired(false);
    rebalanceOption.setArgName("clusterName dbName replicationNo");

    Option nodeInfoOption = OptionBuilder.withLongOpt(nodeInfo)
        .withDescription("Query info of a node in a cluster").create();
    nodeInfoOption.setArgs(2);
    nodeInfoOption.setRequired(false);
    nodeInfoOption.setArgName("clusterName nodeName");

    Option clusterInfoOption = OptionBuilder.withLongOpt(clusterInfo)
        .withDescription("Query info of a cluster").create();
    clusterInfoOption.setArgs(1);
    clusterInfoOption.setRequired(false);
    clusterInfoOption.setArgName("clusterName");

    Option databaseInfoOption = OptionBuilder.withLongOpt(databaseInfo)
        .withDescription("Query info of a database").create();
    databaseInfoOption.setArgs(2);
    databaseInfoOption.setRequired(false);
    databaseInfoOption.setArgName("clusterName databaseName");

    Option partitionInfoOption = OptionBuilder.withLongOpt(partitionInfo)
        .withDescription("Query info of a partition").create();
    partitionInfoOption.setArgs(2);
    partitionInfoOption.setRequired(false);
    partitionInfoOption.setArgName("clusterName partitionName");
    
    Option enableNodeOption = OptionBuilder.withLongOpt(enableNode)
    .withDescription("Enable / disable a node").create();
    enableNodeOption.setArgs(3);
    enableNodeOption.setRequired(false);
    enableNodeOption.setArgName("clusterName nodeName true/false");

    // add an option group including either --zkSvr or --configFile
    Option fileOption = OptionBuilder.withLongOpt(configFile)
        .withDescription("Provide file to write states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to write states/messages (Optional)");

    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);
    optionGroup.addOption(fileOption);

    Options options = new Options();
    options.addOption(helpOption);
    // options.addOption(zkServerOption);
    options.addOption(rebalanceOption);
    options.addOption(addDatabaseOption);
    options.addOption(addClusterOption);
    options.addOption(addNodeOption);
    options.addOption(listNodesOption);
    options.addOption(listDatabaseOption);
    options.addOption(listClustersOption);
    options.addOption(rebalanceOption);
    options.addOption(nodeInfoOption);
    options.addOption(clusterInfoOption);
    options.addOption(databaseInfoOption);
    options.addOption(partitionInfoOption);
    options.addOption(enableNodeOption);

    options.addOptionGroup(optionGroup);

    return options;
  }

  public static int processCommandLineArgs(String[] cliArgs) throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      cmd = cliParser.parse(cliOptions, cliArgs);
    } catch (ParseException pe)
    {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    if (cmd.hasOption(configFile))
    {
      String file = cmd.getOptionValue(configFile);

      // for temporary test only, will move to command line
      // create fake db names
      List<FileBasedClusterManager.DBParam> dbParams = new ArrayList<FileBasedClusterManager.DBParam>();
      dbParams.add(new FileBasedClusterManager.DBParam("BizFollow", 1));
      dbParams.add(new FileBasedClusterManager.DBParam("BizProfile", 1));
      dbParams.add(new FileBasedClusterManager.DBParam("EspressoDB", 10));
      dbParams.add(new FileBasedClusterManager.DBParam("MailboxDB", 128));
      dbParams.add(new FileBasedClusterManager.DBParam("MyDB", 8));
      dbParams.add(new FileBasedClusterManager.DBParam("schemata", 1));
      String[] nodesInfo = { "localhost:8900" };

      ClusterViewSerializer serializer = new ClusterViewSerializer(file);
      ClusterView view = FileBasedClusterManager.generateStaticConfigClusterView(nodesInfo,
          dbParams, 0);

      byte[] bytes;
      bytes = serializer.serialize(view);
      // System.out.println(new String(bytes));

      ClusterView restoredView = (ClusterView) serializer.deserialize(bytes);
      // System.out.println(restoredView);

      bytes = serializer.serialize(restoredView);
      // System.out.println(new String(bytes));

      return 0;
    }

    ClusterSetup setupTool = new ClusterSetup(cmd.getOptionValue(zkServerAddress));

    if (cmd.hasOption(addCluster))
    {
      String clusterName = cmd.getOptionValue(addCluster);
      setupTool.addCluster(clusterName, false);
      return 0;
    }

    if (cmd.hasOption(addNode))
    {
      String clusterName = cmd.getOptionValues(addNode)[0];
      String nodeAddressInfo = cmd.getOptionValues(addNode)[1];
      String[] nodeAddresses = nodeAddressInfo.split(";");
      setupTool.addNodesToCluster(clusterName, nodeAddresses);
      return 0;
    }

    if (cmd.hasOption(addDatabase))
    {
      String clusterName = cmd.getOptionValues(addDatabase)[0];
      String dbName = cmd.getOptionValues(addDatabase)[1];
      int partitions = Integer.parseInt(cmd.getOptionValues(addDatabase)[2]);
      setupTool.addDatabaseToCluster(clusterName, dbName, partitions);
      return 0;
    }

    if (cmd.hasOption(rebalance))
    {
      String clusterName = cmd.getOptionValues(rebalance)[0];
      String dbName = cmd.getOptionValues(rebalance)[1];
      int replicas = Integer.parseInt(cmd.getOptionValues(rebalance)[2]);
      setupTool.rebalanceStorageCluster(clusterName, dbName, replicas);
      return 0;
    }

    if (cmd.hasOption(listClusters))
    {
      List<String> clusters = setupTool.getClusterManagementTool().getClusters();

      System.out.println("Existing clusters:");
      for (String cluster : clusters)
      {
        System.out.println(cluster);
      }
      return 0;
    }

    if (cmd.hasOption(listDatabase))
    {
      String clusterName = cmd.getOptionValue(listDatabase);
      List<String> dbNames = setupTool.getClusterManagementTool()
          .getDatabasesInCluster(clusterName);

      System.out.println("Existing databses in cluster " + clusterName + ":");
      for (String dbName : dbNames)
      {
        System.out.println(dbName);
      }
    } else if (cmd.hasOption(listNodes))
    {
      String clusterName = cmd.getOptionValue(listNodes);
      List<String> nodes = setupTool.getClusterManagementTool().getNodeNamesInCluster(clusterName);

      System.out.println("Nodes in cluster " + clusterName + ":");
      for (String nodeName : nodes)
      {
        System.out.println(nodeName);
      }
    }
    // TODO: add implementation in CM V2
    else if (cmd.hasOption(nodeInfo))
    {
      // print out current states and
    } 
    else if (cmd.hasOption(databaseInfo))
    {
      // print out partition number, db name and replication number
      // Also the ideal states and current states
    } 
    else if (cmd.hasOption(partitionInfo))
    {
      // print out where the partition master / slaves locates
    }
    else if (cmd.hasOption(enableNode))
    {
      String clusterName = cmd.getOptionValues(enableNode)[0];
      String instanceName = cmd.getOptionValues(enableNode)[1];
      boolean enabled  = Boolean.parseBoolean(cmd.getOptionValues(enableNode)[1].toLowerCase());
      
      setupTool.getClusterManagementTool().enableInstance(clusterName, instanceName, enabled);
    }
    return 0;
  }

  /**
   * @param args
   * @throws Exception
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static void main(String[] args) throws Exception
  {
    // For temporary test only, remove later
    Logger.getRootLogger().setLevel(Level.ERROR);
    if (args.length == 0)
    {
      new ClusterSetup("localhost:2181").setupTestCluster("storage-integration-cluster");
      new ClusterSetup("localhost:2181").setupTestCluster("relay-integration-cluster");
      System.exit(0);
    }

    int ret = processCommandLineArgs(args);
    System.exit(ret);
  }
}
