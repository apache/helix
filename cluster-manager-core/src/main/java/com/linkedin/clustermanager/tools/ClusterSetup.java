package com.linkedin.clustermanager.tools;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKClusterManagementTool;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ClusterSetup
{
  public static final String zkServerAddress = "zkSvr";

  // List info about the cluster / DB/ Instances
  public static final String listClusters = "listClusters";
  public static final String listResourceGroups = "listResourceGroups";
  public static final String listInstances = "listInstances";

  // Add and rebalance
  public static final String addCluster = "addCluster";
  public static final String addInstance = "addNode";
  public static final String addResourceGroup = "addResourceGroup";
  public static final String addStateModelDef = "addStateModelDef";
  public static final String rebalance = "rebalance";

  // Query info (TBD in V2)
  public static final String listClusterInfo = "listClusterInfo";
  public static final String listInstanceInfo = "listInstanceInfo";
  public static final String listResourceGroupInfo = "listResourceGroupInfo";
  public static final String listResourceInfo = "listResourceInfo";
  public static final String listStateModels = "listStateModels";
  public static final String listStateModel = "listStateModel";
  

  // TODO: refactor
  // setup for file-based cluster manager
  // public static final String configFile = "configFile";

  // enable / disable Instances
  public static final String enableInstance = "enableInstance";

  public static final String help = "help";

  static Logger _logger = Logger.getLogger(ClusterSetup.class);
  String _zkServerAddress;

  public ClusterSetup(String zkServerAddress)
  {
    _zkServerAddress = zkServerAddress;
  }

  public void addCluster(String clusterName, boolean overwritePrevious)
  {
    ClusterManagementService managementTool = getClusterManagementTool();
    managementTool.addCluster(clusterName, overwritePrevious);
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    addStateModelDef(clusterName, "MasterSlave",
        generator.generateConfigForMasterSlave());
  }

  public void addInstancesToCluster(String clusterName, String[] InstanceInfoArray)
  {
    for (String InstanceInfo : InstanceInfoArray)
    {
      // the storage Instance info must be hostname:port format.
      if (InstanceInfo.length() > 0)
      {
        addInstanceToCluster(clusterName, InstanceInfo);
      }
    }
  }

  public void addInstanceToCluster(String clusterName, String InstanceAddress)
  {
    // InstanceAddress must be in host:port format
    int lastPos = InstanceAddress.lastIndexOf(":");
    if (lastPos <= 0)
    {
      String error = "Invalid storage Instance info format: " + InstanceAddress;
      _logger.warn(error);
      throw new ClusterManagerException(error);
    }
    String host = InstanceAddress.substring(0, lastPos);
    String portStr = InstanceAddress.substring(lastPos + 1);
    int port = Integer.parseInt(portStr);
    addInstanceToCluster(clusterName, host, port);
  }

  public void addInstanceToCluster(String clusterName, String host, int port)
  {
    ClusterManagementService managementTool = getClusterManagementTool();

    ZNRecord InstanceConfig = new ZNRecord();
    String InstanceId = host + "_" + port;
    InstanceConfig.setId(InstanceId);
    InstanceConfig.setSimpleField(InstanceConfigProperty.HOST.toString(), host);
    InstanceConfig
        .setSimpleField(InstanceConfigProperty.PORT.toString(), "" + port);
    InstanceConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(),
        true + "");

    managementTool.addInstance(clusterName, InstanceConfig);
  }

  public ClusterManagementService getClusterManagementTool()
  {
    ZkClient zkClient = ZKClientPool.getZkClient(_zkServerAddress);
    return new ZKClusterManagementTool(zkClient);
  }

  public void addStateModelDef(String clusterName, String stateModelDef,
      ZNRecord record)
  {
    ClusterManagementService managementTool = getClusterManagementTool();
    managementTool.addStateModelDef(clusterName, stateModelDef, record);
  }

  public void addResourceGroupToCluster(String clusterName,
      String resourceGroup, int numResources, String stateModelRef)
  {
    ClusterManagementService managementTool = getClusterManagementTool();
    managementTool.addResourceGroup(clusterName, resourceGroup, numResources,
        stateModelRef);
  }
  
  public void dropResourceGroupToCluster(String clusterName,
      String resourceGroup)
  {
    ClusterManagementService managementTool = getClusterManagementTool();
    managementTool.dropResourceGroup(clusterName, resourceGroup);
  }

  public void rebalanceStorageCluster(String clusterName,
      String resourceGroupName, int replica)
  {
    ClusterManagementService managementTool = getClusterManagementTool();
    List<String> InstanceNames = managementTool.getInstancesInCluster(clusterName);

    ZNRecord dbIdealState = managementTool.getResourceGroupIdealState(
        clusterName, resourceGroupName);
    int partitions = Integer
        .parseInt(dbIdealState.getSimpleField("partitions"));

    ZNRecord idealState = IdealStateCalculatorForStorageNode
        .calculateIdealState(InstanceNames, partitions, replica, resourceGroupName);
    idealState.merge(dbIdealState);
    managementTool.setResourceGroupIdealState(clusterName, resourceGroupName,
        idealState);
  }

  /**
   * Sets up a cluster with 6 Instances[localhost:8900 to localhost:8905], 1
   * resourceGroup[EspressoDB] with a replication factor of 3
   * 
   * @param clusterName
   */
  public void setupTestCluster(String clusterName)
  {
    addCluster(clusterName, true);
    String storageInstanceInfoArray[] = new String[6];
    for (int i = 0; i < storageInstanceInfoArray.length; i++)
    {
      storageInstanceInfoArray[i] = "localhost:" + (8900 + i);
    }
    addInstancesToCluster(clusterName, storageInstanceInfoArray);
    addResourceGroupToCluster(clusterName, "TestDB", 10, "MasterSlave");
    rebalanceStorageCluster(clusterName, "TestDB", 3);
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

    Option listResourceGroupOption = OptionBuilder
        .withLongOpt(listResourceGroups)
        .withDescription("List resourceGroups hosted in a cluster").create();
    listResourceGroupOption.setArgs(1);
    listResourceGroupOption.setRequired(false);
    listResourceGroupOption.setArgName("clusterName");

    Option listInstancesOption = OptionBuilder.withLongOpt(listInstances)
        .withDescription("List Instances in a cluster").create();
    listInstancesOption.setArgs(1);
    listInstancesOption.setRequired(false);
    listInstancesOption.setArgName("clusterName");

    Option addClusterOption = OptionBuilder.withLongOpt(addCluster)
        .withDescription("Add a new cluster").create();
    addClusterOption.setArgs(1);
    addClusterOption.setRequired(false);
    addClusterOption.setArgName("clusterName");

    Option addInstanceOption = OptionBuilder.withLongOpt(addInstance)
        .withDescription("Add a new Instance to a cluster").create();
    addInstanceOption.setArgs(2);
    addInstanceOption.setRequired(false);
    addInstanceOption.setArgName("clusterName InstanceAddress(host:port)");

    Option addResourceGroupOption = OptionBuilder.withLongOpt(addResourceGroup)
        .withDescription("Add a resourceGroup to a cluster").create();
    addResourceGroupOption.setArgs(4);
    addResourceGroupOption.setRequired(false);
    addResourceGroupOption
        .setArgName("clusterName resourceGroupName partitionNo stateModelRef");

    Option addStateModelDefGroupOption = OptionBuilder
        .withLongOpt(addStateModelDef)
        .withDescription("Add a resourceGroup to a cluster").create();
    addStateModelDefGroupOption.setArgs(3);
    addStateModelDefGroupOption.setRequired(false);
    addStateModelDefGroupOption.setArgName("clusterName stateModelDef <text>");

    Option rebalanceOption = OptionBuilder.withLongOpt(rebalance)
        .withDescription("Rebalance a resourceGroup in a cluster").create();
    rebalanceOption.setArgs(3);
    rebalanceOption.setRequired(false);
    rebalanceOption.setArgName("clusterName resourceGroupName replicationNo");

    Option InstanceInfoOption = OptionBuilder.withLongOpt(listInstanceInfo)
        .withDescription("Query info of a Instance in a cluster").create();
    InstanceInfoOption.setArgs(2);
    InstanceInfoOption.setRequired(false);
    InstanceInfoOption.setArgName("clusterName InstanceName");

    Option clusterInfoOption = OptionBuilder.withLongOpt(listClusterInfo)
        .withDescription("Query info of a cluster").create();
    clusterInfoOption.setArgs(1);
    clusterInfoOption.setRequired(false);
    clusterInfoOption.setArgName("clusterName");

    Option resourceGroupInfoOption = OptionBuilder
        .withLongOpt(listResourceGroupInfo)
        .withDescription("Query info of a resourceGroup").create();
    resourceGroupInfoOption.setArgs(2);
    resourceGroupInfoOption.setRequired(false);
    resourceGroupInfoOption.setArgName("clusterName resourceGroupName");

    Option partitionInfoOption = OptionBuilder.withLongOpt(listResourceInfo)
        .withDescription("Query info of a partition").create();
    partitionInfoOption.setArgs(2);
    partitionInfoOption.setRequired(false);
    partitionInfoOption.setArgName("clusterName partitionName");

    Option enableInstanceOption = OptionBuilder.withLongOpt(enableInstance)
        .withDescription("Enable / disable a Instance").create();
    enableInstanceOption.setArgs(3);
    enableInstanceOption.setRequired(false);
    enableInstanceOption.setArgName("clusterName InstanceName true/false");
    
    Option listStateModelsOption = OptionBuilder.withLongOpt(listStateModels)
      .withDescription("Query info of state models in a cluster").create();
    listStateModelsOption.setArgs(1);
    listStateModelsOption.setRequired(false);
    listStateModelsOption.setArgName("clusterName");

    Option listStateModelOption = OptionBuilder.withLongOpt(listStateModel)
      .withDescription("Query info of a state model in a cluster").create();
    listStateModelOption.setArgs(2);
    listStateModelOption.setRequired(false);
    listStateModelOption.setArgName("clusterName stateModelName");


    // add an option group including either --zkSvr or --configFile
    /**
    Option fileOption = OptionBuilder.withLongOpt(configFile)
        .withDescription("Provide file to write states/messages").create();
    fileOption.setArgs(1);
    fileOption.setRequired(true);
    fileOption.setArgName("File to write states/messages (Optional)");
     **/
    OptionGroup optionGroup = new OptionGroup();
    optionGroup.addOption(zkServerOption);
    // optionGroup.addOption(fileOption);

    Options options = new Options();
    options.addOption(helpOption);
    // options.addOption(zkServerOption);
    options.addOption(rebalanceOption);
    options.addOption(addResourceGroupOption);
    options.addOption(addClusterOption);
    options.addOption(addInstanceOption);
    options.addOption(listInstancesOption);
    options.addOption(listResourceGroupOption);
    options.addOption(listClustersOption);
    options.addOption(rebalanceOption);
    options.addOption(InstanceInfoOption);
    options.addOption(clusterInfoOption);
    options.addOption(resourceGroupInfoOption);
    options.addOption(partitionInfoOption);
    options.addOption(enableInstanceOption);
    options.addOption(listStateModelsOption);
    options.addOption(listStateModelOption);

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
      System.err
          .println("CommandLineClient: failed to parse command-line options: "
              + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    /**
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
      String[] InstancesInfo =
      { "localhost:8900" };

      // ClusterViewSerializer serializer = new ClusterViewSerializer(file);
      int replica = 0;
      ClusterView view = FileBasedClusterManager
          .generateStaticConfigClusterView(InstancesInfo, dbParams, replica);

      // byte[] bytes;
      ClusterViewSerializer.serialize(view, new File(file));
      // System.out.println(new String(bytes));

      ClusterView restoredView = ClusterViewSerializer.deserialize(new File(
          file));
      // System.out.println(restoredView);

      byte[] bytes = ClusterViewSerializer.serialize(restoredView);
      // System.out.println(new String(bytes));

      return 0;
    }
    **/
    
    ClusterSetup setupTool = new ClusterSetup(
        cmd.getOptionValue(zkServerAddress));

    if (cmd.hasOption(addCluster))
    {
      String clusterName = cmd.getOptionValue(addCluster);
      setupTool.addCluster(clusterName, false);
      return 0;
    }

    if (cmd.hasOption(addInstance))
    {
      String clusterName = cmd.getOptionValues(addInstance)[0];
      String InstanceAddressInfo = cmd.getOptionValues(addInstance)[1];
      String[] InstanceAddresses = InstanceAddressInfo.split(";");
      setupTool.addInstancesToCluster(clusterName, InstanceAddresses);
      return 0;
    }

    if (cmd.hasOption(addResourceGroup))
    {
      String clusterName = cmd.getOptionValues(addResourceGroup)[0];
      String resourceGroupName = cmd.getOptionValues(addResourceGroup)[1];
      int partitions = Integer
          .parseInt(cmd.getOptionValues(addResourceGroup)[2]);
      String stateModelRef = cmd.getOptionValues(addResourceGroup)[3];
      setupTool.addResourceGroupToCluster(clusterName, resourceGroupName,
          partitions, stateModelRef);
      return 0;
    }

    if (cmd.hasOption(rebalance))
    {
      String clusterName = cmd.getOptionValues(rebalance)[0];
      String resourceGroupName = cmd.getOptionValues(rebalance)[1];
      int replicas = Integer.parseInt(cmd.getOptionValues(rebalance)[2]);
      setupTool.rebalanceStorageCluster(clusterName, resourceGroupName,
          replicas);
      return 0;
    }

    if (cmd.hasOption(listClusters))
    {
      List<String> clusters = setupTool.getClusterManagementTool()
          .getClusters();

      System.out.println("Existing clusters:");
      for (String cluster : clusters)
      {
        System.out.println(cluster);
      }
      return 0;
    }

    if (cmd.hasOption(listResourceGroups))
    {
      String clusterName = cmd.getOptionValue(listResourceGroups);
      List<String> resourceGroupNames = setupTool.getClusterManagementTool()
          .getResourceGroupsInCluster(clusterName);

      System.out.println("Existing resources in cluster " + clusterName + ":");
      for (String resourceGroupName : resourceGroupNames)
      {
        System.out.println(resourceGroupName);
      }
      return 0;
    } 
    else if(cmd.hasOption(listClusterInfo))
    {
      String clusterName = cmd.getOptionValue(listClusterInfo);
      List<String> resourceGroupNames = setupTool.getClusterManagementTool()
          .getResourceGroupsInCluster(clusterName);
      List<String> Instances = setupTool.getClusterManagementTool()
        .getInstancesInCluster(clusterName);
      
      System.out.println("Existing resources in cluster " + clusterName + ":");
      for (String resourceGroupName : resourceGroupNames)
      {
        System.out.println(resourceGroupName);
      }
      
      System.out.println("Instances in cluster " + clusterName + ":");
      for (String InstanceName : Instances)
      {
        System.out.println(InstanceName);
      }
      return 0;
    }
    else if (cmd.hasOption(listInstances))
    {
      String clusterName = cmd.getOptionValue(listInstances);
      List<String> Instances = setupTool.getClusterManagementTool()
          .getInstancesInCluster(clusterName);

      System.out.println("Instances in cluster " + clusterName + ":");
      for (String InstanceName : Instances)
      {
        System.out.println(InstanceName);
      }
      return 0;
    }
    else if (cmd.hasOption(listInstanceInfo))
    {
      String clusterName = cmd.getOptionValues(listInstanceInfo)[0];
      String instanceName = cmd.getOptionValues(listInstanceInfo)[1];
      ZNRecord record = setupTool.getClusterManagementTool().getInstanceConfig(clusterName, instanceName);
      
      String result = new String(new ZNRecordSerializer().serialize(record));
      System.out.println(result);
      return 0;
      
      // print out current states and
    } 
    else if (cmd.hasOption(listResourceGroupInfo))
    {
      // print out partition number, db name and replication number
      // Also the ideal states and current states
      String clusterName = cmd.getOptionValues(listResourceGroupInfo)[0];
      String resourceGroupName = cmd.getOptionValues(listResourceGroupInfo)[1];
      ZNRecord idealState = setupTool.getClusterManagementTool().getResourceGroupIdealState(clusterName, resourceGroupName);
      ZNRecord externalView = setupTool.getClusterManagementTool().getResourceGroupExternalView(clusterName, resourceGroupName);
      
      System.out.println("IdealState for "+resourceGroupName+":");
      System.out.println(new String(new ZNRecordSerializer().serialize(idealState)));
      
      System.out.println();
      System.out.println("External view for "+resourceGroupName+":");
      System.out.println(new String(new ZNRecordSerializer().serialize(externalView)));
      return 0;
      
    } 
    else if (cmd.hasOption(listResourceInfo))
    {
      // print out where the partition master / slaves locates
    } 
    else if (cmd.hasOption(enableInstance))
    {
      String clusterName = cmd.getOptionValues(enableInstance)[0];
      String instanceName = cmd.getOptionValues(enableInstance)[1];
      boolean enabled = Boolean.parseBoolean(cmd.getOptionValues(enableInstance)[1]
          .toLowerCase());

      setupTool.getClusterManagementTool().enableInstance(clusterName,
          instanceName, enabled);
      return 0;
    } 
    else if(cmd.hasOption(listStateModels))
    {
      String clusterName = cmd.getOptionValues(listStateModels)[0];
      
      List<String> stateModels =  setupTool.getClusterManagementTool()
      .getStateModelDefs(clusterName);

      System.out.println("Existing state models:");
      for (String stateModel : stateModels)
      {
        System.out.println(stateModel);
      }
      return 0;
    }
    else if (cmd.hasOption(listStateModel))
    {
      String clusterName = cmd.getOptionValues(listStateModel)[0];
      String stateModel = cmd.getOptionValues(listStateModel)[1];
      ZNRecord record = setupTool.getClusterManagementTool().getStateModelDef(clusterName, stateModel);
      String result = new String(new ZNRecordSerializer().serialize(record));
      System.out.println(result);
    }
    else if (cmd.hasOption(help))
    {
      printUsage(cliOptions);
      return 0;
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
      new ClusterSetup("localhost:2181")
          .setupTestCluster("storage-integration-cluster");
      new ClusterSetup("localhost:2181")
          .setupTestCluster("relay-integration-cluster");
      System.exit(0);
    }

    int ret = processCommandLineArgs(args);
    System.exit(ret);
  }
}
