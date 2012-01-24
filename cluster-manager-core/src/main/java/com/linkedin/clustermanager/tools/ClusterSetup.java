package com.linkedin.clustermanager.tools;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.zk.ZKClusterManagementTool;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.agent.zk.ZNRecordSerializer;
import com.linkedin.clustermanager.agent.zk.ZkClient;
import com.linkedin.clustermanager.model.ExternalView;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.IdealState.IdealStateModeProperty;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.StateModelDefinition;
import com.linkedin.clustermanager.util.ZKClientPool;

public class ClusterSetup
{
  private static Logger      logger                = Logger.getLogger(ClusterSetup.class);
  public static final String zkServerAddress       = "zkSvr";

  // List info about the cluster / DB/ Instances
  public static final String listClusters          = "listClusters";
  public static final String listResourceGroups    = "listResourceGroups";
  public static final String listInstances         = "listInstances";

  // Add, drop, and rebalance
  public static final String addCluster            = "addCluster";
  public static final String dropCluster         = "dropCluster";
  public static final String addInstance           = "addNode";
  public static final String addResourceGroup      = "addResourceGroup";
  public static final String addStateModelDef      = "addStateModelDef";
  public static final String addIdealState         = "addIdealState";
  public static final String disableInstance       = "disableNode";
  public static final String dropInstance       = "dropNode";
  public static final String rebalance             = "rebalance";

  // Query info (TBD in V2)
  public static final String listClusterInfo       = "listClusterInfo";
  public static final String listInstanceInfo      = "listInstanceInfo";
  public static final String listResourceGroupInfo = "listResourceGroupInfo";
  public static final String listResourceInfo      = "listResourceInfo";
  public static final String listStateModels       = "listStateModels";
  public static final String listStateModel        = "listStateModel";

  // enable / disable Instances
  public static final String enableInstance        = "enableInstance";
  public static final String help                  = "help";
  
  // stats /alerts
  public static final String addStat               = "addStat";
  public static final String addAlert              = "addAlert";
  
  static Logger              _logger               = Logger.getLogger(ClusterSetup.class);
  String                     _zkServerAddress;
  ZkClient _zkClient;
  ClusterManagementService _managementService;

  public ClusterSetup(String zkServerAddress)
  {
    _zkServerAddress = zkServerAddress;
    _zkClient  = ZKClientPool.getZkClient(_zkServerAddress);
    _managementService = new ZKClusterManagementTool(_zkClient);
  }

  public void addCluster(String clusterName, boolean overwritePrevious)
  {
    _managementService.addCluster(clusterName, overwritePrevious);

    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    addStateModelDef(clusterName, "MasterSlave", new StateModelDefinition(generator.generateConfigForMasterSlave()));
    addStateModelDef(clusterName, "LeaderStandby", new StateModelDefinition(generator.generateConfigForLeaderStandby()));
    addStateModelDef(clusterName, "StorageSchemata", new StateModelDefinition(generator.generateConfigForStorageSchemata()));
    addStateModelDef(clusterName, "OnlineOffline", new StateModelDefinition(generator.generateConfigForOnlineOffline()));
  }
  
  public void deleteCluster(String clusterName)
  {
    _managementService.dropCluster(clusterName);
  }

  public void addCluster(String clusterName,
                         boolean overwritePrevious,
                         String stateModDefName,
                         StateModelDefinition stateModDef)
  {
    _managementService.addCluster(clusterName, overwritePrevious);
    addStateModelDef(clusterName, stateModDefName, stateModDef);
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
    int lastPos = InstanceAddress.lastIndexOf("_");
    if (lastPos <= 0)
    {
      lastPos = InstanceAddress.lastIndexOf(":");
    }
    if(lastPos <= 0)
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
    String instanceId = host + "_" + port;
    InstanceConfig config = new InstanceConfig(instanceId);
    config.setHostName(host);
    config.setPort(Integer.toString(port));
    config.setInstanceEnabled(true);
    _managementService.addInstance(clusterName, config);
  }

  public void dropInstancesFromCluster(String clusterName, String[] InstanceInfoArray)
  {
    for (String InstanceInfo : InstanceInfoArray)
    {
      // the storage Instance info must be hostname:port format.
      if (InstanceInfo.length() > 0)
      {
        dropInstanceFromCluster(clusterName, InstanceInfo);
      }
    }
  }

  public void dropInstanceFromCluster(String clusterName, String InstanceAddress)
  {
    // InstanceAddress must be in host:port format
    int lastPos = InstanceAddress.lastIndexOf("_");
    if (lastPos <= 0)
    {
      lastPos = InstanceAddress.lastIndexOf(":");
    }
    if (lastPos <= 0)
    {
      String error = "Invalid storage Instance info format: " + InstanceAddress;
      _logger.warn(error);
      throw new ClusterManagerException(error);
    }
    String host = InstanceAddress.substring(0, lastPos);
    String portStr = InstanceAddress.substring(lastPos + 1);
    int port = Integer.parseInt(portStr);
    dropInstanceFromCluster(clusterName, host, port);
  }

  public void dropInstanceFromCluster(String clusterName, String host, int port)
  {
    String instanceId = host + "_" + port;

    ZkClient zkClient = ZKClientPool.getZkClient(_zkServerAddress);
    InstanceConfig config = new ZKDataAccessor(clusterName, zkClient).getProperty(InstanceConfig.class,
                                                                                  PropertyType.CONFIGS,
                                                                                  instanceId);
    if (config == null)
    {
      String error = "Node "+instanceId+" does not exist, cannot drop";
      _logger.warn(error);
      throw new ClusterManagerException(error);
    }

    // ensure node is disabled, otherwise fail
    if (config.getInstanceEnabled())
    {
      String error = "Node " + instanceId + " is enabled, cannot drop";
      _logger.warn(error);
      throw new ClusterManagerException(error);
    }
    _managementService.dropInstance(clusterName, config);
  }

  public ClusterManagementService getClusterManagementTool()
  {
    return _managementService;
  }

  public void addStateModelDef(String clusterName, String stateModelDef, StateModelDefinition record)
  {
    _managementService.addStateModelDef(clusterName, stateModelDef, record);
  }

  public void addResourceGroupToCluster(String clusterName,
                                        String resourceGroup,
                                        int numResources,
                                        String stateModelRef)
  {
    addResourceGroupToCluster(clusterName,
                              resourceGroup,
                              numResources,
                              stateModelRef,
                              IdealStateModeProperty.AUTO.toString());
  }

  public void addResourceGroupToCluster(String clusterName,
                                        String resourceGroup,
                                        int numResources,
                                        String stateModelRef,
                                        String idealStateMode)
  {
    if (!idealStateMode.equalsIgnoreCase(IdealStateModeProperty.CUSTOMIZED.toString()))
    {
      logger.info("ideal state mode is configured to auto for " + resourceGroup);
      idealStateMode = IdealStateModeProperty.AUTO.toString();
    }
    _managementService.addResourceGroup(clusterName,
                                    resourceGroup,
                                    numResources,
                                    stateModelRef,
                                    idealStateMode);
  }

  public void dropResourceGroupToCluster(String clusterName, String resourceGroup)
  {
    _managementService.dropResourceGroup(clusterName, resourceGroup);
  }

  public void rebalanceStorageCluster(String clusterName, String resourceGroupName, int replica)
  {
    List<String> InstanceNames = _managementService.getInstancesInCluster(clusterName);

    IdealState idealState =
      _managementService.getResourceGroupIdealState(clusterName, resourceGroupName);
    idealState.setReplicas(replica);
    int partitions = idealState.getNumPartitions();
    String stateModelName = idealState.getStateModelDefRef();
    StateModelDefinition stateModDef = _managementService.getStateModelDef(clusterName, stateModelName);

    if (stateModDef == null)
    {
      throw new ClusterManagerException("cannot find state model: " + stateModelName);
    }
//    StateModelDefinition def = new StateModelDefinition(stateModDef);

    List<String> statePriorityList = stateModDef.getStatesPriorityList();

    String masterStateValue = null;
    String slaveStateValue = null;
    replica--;

    for (String state : statePriorityList)
    {
      String count = stateModDef.getNumInstancesPerState(state);
      if (count.equals("1"))
      {
        if (masterStateValue != null)
        {
          throw new ClusterManagerException("Invalid or unsupported state model definition");
        }
        masterStateValue = state;
      }
      else if (count.equalsIgnoreCase("R"))
      {
        if (slaveStateValue != null)
        {
          throw new ClusterManagerException("Invalid or unsupported state model definition");
        }
        slaveStateValue = state;
      }
      else if (count.equalsIgnoreCase("N"))
      {
        if (!(masterStateValue == null && slaveStateValue == null))
        {
          throw new ClusterManagerException("Invalid or unsupported state model definition");
        }
        replica = InstanceNames.size() - 1;
        masterStateValue = slaveStateValue = state;
      }
    }
    if (masterStateValue == null && slaveStateValue == null)
    {
      throw new ClusterManagerException("Invalid or unsupported state model definition");
    }

    if (masterStateValue == null)
    {
      masterStateValue = slaveStateValue;
    }

    ZNRecord newIdealState =
        IdealStateCalculatorForStorageNode.calculateIdealState(InstanceNames,
                                                               partitions,
                                                               replica,
                                                               resourceGroupName,
                                                               masterStateValue,
                                                               slaveStateValue);
    idealState.getRecord().setMapFields(newIdealState.getMapFields());
    idealState.getRecord().setListFields(newIdealState.getListFields());
    _managementService.setResourceGroupIdealState(clusterName, resourceGroupName, idealState);
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
    helpFormatter.setWidth(1000);
    helpFormatter.printHelp("java " + ClusterSetup.class.getName(), cliOptions);
  }

  @SuppressWarnings("static-access")
  private static Options constructCommandLineOptions()
  {
    Option helpOption =
        OptionBuilder.withLongOpt(help)
                     .withDescription("Prints command-line options info")
                     .create();

    Option zkServerOption =
        OptionBuilder.withLongOpt(zkServerAddress)
                     .withDescription("Provide zookeeper address")
                     .create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("ZookeeperServerAddress(Required)");



    Option listClustersOption =
        OptionBuilder.withLongOpt(listClusters).withDescription("List existing clusters").create();
    listClustersOption.setArgs(0);
    listClustersOption.setRequired(false);

    Option listResourceGroupOption =
        OptionBuilder.withLongOpt(listResourceGroups)
                     .withDescription("List resourceGroups hosted in a cluster")
                     .create();
    listResourceGroupOption.setArgs(1);
    listResourceGroupOption.setRequired(false);
    listResourceGroupOption.setArgName("clusterName");

    Option listInstancesOption =
        OptionBuilder.withLongOpt(listInstances)
                     .withDescription("List Instances in a cluster")
                     .create();
    listInstancesOption.setArgs(1);
    listInstancesOption.setRequired(false);
    listInstancesOption.setArgName("clusterName");

    Option addClusterOption =
        OptionBuilder.withLongOpt(addCluster).withDescription("Add a new cluster").create();
    addClusterOption.setArgs(1);
    addClusterOption.setRequired(false);
    addClusterOption.setArgName("clusterName");
    
    Option deleteClusterOption =
        OptionBuilder.withLongOpt(dropCluster).withDescription("Delete a cluster").create();
    deleteClusterOption.setArgs(1);
    deleteClusterOption.setRequired(false);
    deleteClusterOption.setArgName("clusterName");

    Option addInstanceOption =
        OptionBuilder.withLongOpt(addInstance)
                     .withDescription("Add a new Instance to a cluster")
                     .create();
    addInstanceOption.setArgs(2);
    addInstanceOption.setRequired(false);
    addInstanceOption.setArgName("clusterName InstanceAddress(host:port)");

    Option addResourceGroupOption =
        OptionBuilder.withLongOpt(addResourceGroup)
                     .withDescription("Add a resourceGroup to a cluster")
                     .create();
    addResourceGroupOption.setArgs(4);
    addResourceGroupOption.setRequired(false);
    addResourceGroupOption.setArgName("clusterName resourceGroupName partitionNo stateModelRef");

    Option addStateModelDefOption =
        OptionBuilder.withLongOpt(addStateModelDef)
                     .withDescription("Add a State model to a cluster")
                     .create();
    addStateModelDefOption.setArgs(2);
    addStateModelDefOption.setRequired(false);
    addStateModelDefOption.setArgName("clusterName <filename>");

    Option addIdealStateOption =
        OptionBuilder.withLongOpt(addIdealState)
                     .withDescription("Add a State model to a cluster")
                     .create();
    addIdealStateOption.setArgs(3);
    addIdealStateOption.setRequired(false);
    addIdealStateOption.setArgName("clusterName reourceGroupName <filename>");

    Option dropInstanceOption =
        OptionBuilder.withLongOpt(dropInstance)
                     .withDescription("Drop an existing Instance from a cluster")
                     .create();
    dropInstanceOption.setArgs(2);
    dropInstanceOption.setRequired(false);
    dropInstanceOption.setArgName("clusterName InstanceAddress(host:port)");


    Option rebalanceOption =
        OptionBuilder.withLongOpt(rebalance)
                     .withDescription("Rebalance a resourceGroup in a cluster")
                     .create();
    rebalanceOption.setArgs(3);
    rebalanceOption.setRequired(false);
    rebalanceOption.setArgName("clusterName resourceGroupName replicationNo");

    Option InstanceInfoOption =
        OptionBuilder.withLongOpt(listInstanceInfo)
                     .withDescription("Query info of a Instance in a cluster")
                     .create();
    InstanceInfoOption.setArgs(2);
    InstanceInfoOption.setRequired(false);
    InstanceInfoOption.setArgName("clusterName InstanceName");

    Option clusterInfoOption =
        OptionBuilder.withLongOpt(listClusterInfo)
                     .withDescription("Query info of a cluster")
                     .create();
    clusterInfoOption.setArgs(1);
    clusterInfoOption.setRequired(false);
    clusterInfoOption.setArgName("clusterName");

    Option resourceGroupInfoOption =
        OptionBuilder.withLongOpt(listResourceGroupInfo)
                     .withDescription("Query info of a resourceGroup")
                     .create();
    resourceGroupInfoOption.setArgs(2);
    resourceGroupInfoOption.setRequired(false);
    resourceGroupInfoOption.setArgName("clusterName resourceGroupName");

    Option partitionInfoOption =
        OptionBuilder.withLongOpt(listResourceInfo)
                     .withDescription("Query info of a partition")
                     .create();
    partitionInfoOption.setArgs(2);
    partitionInfoOption.setRequired(false);
    partitionInfoOption.setArgName("clusterName partitionName");

    Option enableInstanceOption =
        OptionBuilder.withLongOpt(enableInstance)
                     .withDescription("Enable / disable a Instance")
                     .create();
    enableInstanceOption.setArgs(3);
    enableInstanceOption.setRequired(false);
    enableInstanceOption.setArgName("clusterName InstanceName true/false");

    Option listStateModelsOption =
        OptionBuilder.withLongOpt(listStateModels)
                     .withDescription("Query info of state models in a cluster")
                     .create();
    listStateModelsOption.setArgs(1);
    listStateModelsOption.setRequired(false);
    listStateModelsOption.setArgName("clusterName");

    Option listStateModelOption =
        OptionBuilder.withLongOpt(listStateModel)
                     .withDescription("Query info of a state model in a cluster")
                     .create();
    listStateModelOption.setArgs(2);
    listStateModelOption.setRequired(false);
    listStateModelOption.setArgName("clusterName stateModelName");
    
    Option addStatOption =
            OptionBuilder.withLongOpt(addStat)
                         .withDescription("Add a persistent stat")
                         .create();
        addStatOption.setArgs(2);
        addStatOption.setRequired(false);
        addStatOption.setArgName("clusterName statName");
    Option addAlertOption =
        OptionBuilder.withLongOpt(addAlert)
        .withDescription("Add an alert")
        .create();
    addAlertOption.setArgs(2);
    addAlertOption.setRequired(false);
    addAlertOption.setArgName("clusterName alertName");

    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(rebalanceOption);
    group.addOption(addResourceGroupOption);
    group.addOption(addClusterOption);
    group.addOption(deleteClusterOption);
    group.addOption(addInstanceOption);
    group.addOption(listInstancesOption);
    group.addOption(listResourceGroupOption);
    group.addOption(listClustersOption);
    group.addOption(addIdealStateOption);
    group.addOption(rebalanceOption);
    group.addOption(dropInstanceOption);
    group.addOption(InstanceInfoOption);
    group.addOption(clusterInfoOption);
    group.addOption(resourceGroupInfoOption);
    group.addOption(partitionInfoOption);
    group.addOption(enableInstanceOption);
    group.addOption(addStateModelDefOption);
    group.addOption(listStateModelsOption);
    group.addOption(listStateModelOption);
    group.addOption(addStatOption);
    group.addOption(addAlertOption);
    
    Options options = new Options();
    options.addOption(helpOption);
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    return options;
  }

  private static byte[] readFile(String filePath) throws IOException
  {
    File file = new File(filePath);

    int size = (int) file.length();
    byte[] bytes = new byte[size];
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    int read = 0;
    int numRead = 0;
    while (read < bytes.length && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0)
    {
      read = read + numRead;
    }
    return bytes;
  }

  public static int processCommandLineArgs(String[] cliArgs) throws Exception
  {
    CommandLineParser cliParser = new GnuParser();
    Options cliOptions = constructCommandLineOptions();
    CommandLine cmd = null;

    try
    {
      cmd = cliParser.parse(cliOptions, cliArgs);
    }
    catch (ParseException pe)
    {
      System.err.println("CommandLineClient: failed to parse command-line options: "
          + pe.toString());
      printUsage(cliOptions);
      System.exit(1);
    }

    ClusterSetup setupTool = new ClusterSetup(cmd.getOptionValue(zkServerAddress));

    if (cmd.hasOption(addCluster))
    {
      String clusterName = cmd.getOptionValue(addCluster);
      setupTool.addCluster(clusterName, false);
      return 0;
    }
    
    if (cmd.hasOption(dropCluster))
    {
      String clusterName = cmd.getOptionValue(dropCluster);
      setupTool.deleteCluster(clusterName);
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
      int partitions = Integer.parseInt(cmd.getOptionValues(addResourceGroup)[2]);
      String stateModelRef = cmd.getOptionValues(addResourceGroup)[3];
      setupTool.addResourceGroupToCluster(clusterName, resourceGroupName, partitions, stateModelRef);
      return 0;
    }

    if (cmd.hasOption(rebalance))
    {
      String clusterName = cmd.getOptionValues(rebalance)[0];
      String resourceGroupName = cmd.getOptionValues(rebalance)[1];
      int replicas = Integer.parseInt(cmd.getOptionValues(rebalance)[2]);
      setupTool.rebalanceStorageCluster(clusterName, resourceGroupName, replicas);
      return 0;
    }

    if (cmd.hasOption(dropInstance))
    {
      String clusterName = cmd.getOptionValues(dropInstance)[0];
      String InstanceAddressInfo = cmd.getOptionValues(dropInstance)[1];
      String[] InstanceAddresses = InstanceAddressInfo.split(";");
      setupTool.dropInstancesFromCluster(clusterName, InstanceAddresses);
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

    if (cmd.hasOption(listResourceGroups))
    {
      String clusterName = cmd.getOptionValue(listResourceGroups);
      List<String> resourceGroupNames =
          setupTool.getClusterManagementTool().getResourceGroupsInCluster(clusterName);

      System.out.println("Existing resources in cluster " + clusterName + ":");
      for (String resourceGroupName : resourceGroupNames)
      {
        System.out.println(resourceGroupName);
      }
      return 0;
    }
    else if (cmd.hasOption(listClusterInfo))
    {
      String clusterName = cmd.getOptionValue(listClusterInfo);
      List<String> resourceGroupNames =
          setupTool.getClusterManagementTool().getResourceGroupsInCluster(clusterName);
      List<String> Instances =
          setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

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
      List<String> Instances =
          setupTool.getClusterManagementTool().getInstancesInCluster(clusterName);

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
      InstanceConfig config =
          setupTool.getClusterManagementTool().getInstanceConfig(clusterName, instanceName);

      String result = new String(new ZNRecordSerializer().serialize(config.getRecord()));
      System.out.println("InstanceConfig: " + result);
      return 0;
    }
    else if (cmd.hasOption(listResourceGroupInfo))
    {
      // print out partition number, db name and replication number
      // Also the ideal states and current states
      String clusterName = cmd.getOptionValues(listResourceGroupInfo)[0];
      String resourceGroupName = cmd.getOptionValues(listResourceGroupInfo)[1];
      IdealState idealState =
          setupTool.getClusterManagementTool().getResourceGroupIdealState(clusterName,
                                                                          resourceGroupName);
      ExternalView externalView =
          setupTool.getClusterManagementTool().getResourceGroupExternalView(clusterName,
                                                                            resourceGroupName);

      System.out.println("IdealState for " + resourceGroupName + ":");
      System.out.println(new String(new ZNRecordSerializer().serialize(idealState.getRecord())));

      System.out.println();
      System.out.println("External view for " + resourceGroupName + ":");
      System.out.println(new String(new ZNRecordSerializer().serialize(externalView.getRecord())));
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
      boolean enabled = Boolean.parseBoolean(cmd.getOptionValues(enableInstance)[2].toLowerCase());

      setupTool.getClusterManagementTool().enableInstance(clusterName, instanceName, enabled);
      return 0;
    }
    else if (cmd.hasOption(listStateModels))
    {
      String clusterName = cmd.getOptionValues(listStateModels)[0];

      List<String> stateModels =
          setupTool.getClusterManagementTool().getStateModelDefs(clusterName);

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
      StateModelDefinition stateModelDef =
          setupTool.getClusterManagementTool().getStateModelDef(clusterName, stateModel);
      String result = new String(new ZNRecordSerializer().serialize(stateModelDef.getRecord()));
      System.out.println("StateModelDefinition: " + result);
      return 0;
    }
    else if (cmd.hasOption(addStateModelDef))
    {
      String clusterName = cmd.getOptionValues(addStateModelDef)[0];
      String stateModelFile = cmd.getOptionValues(addStateModelDef)[1];

      ZNRecord stateModelRecord =
          (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(stateModelFile)));
      if (stateModelRecord.getId() == null || stateModelRecord.getId().length() == 0)
      {
        throw new IllegalArgumentException("ZNRecord for state model definition must have an id");
      }
      setupTool.getClusterManagementTool().addStateModelDef(clusterName,
                                                            stateModelRecord.getId(),
                                                            new StateModelDefinition(stateModelRecord));
      return 0;
    }
    else if (cmd.hasOption(addIdealState))
    {
      String clusterName = cmd.getOptionValues(addIdealState)[0];
      String resourceGroupName = cmd.getOptionValues(addIdealState)[1];
      String idealStateFile = cmd.getOptionValues(addIdealState)[2];

      ZNRecord idealStateRecord =
          (ZNRecord) (new ZNRecordSerializer().deserialize(readFile(idealStateFile)));
      if (idealStateRecord.getId() == null || !idealStateRecord.getId().equals(resourceGroupName))
      {
        throw new IllegalArgumentException("ideal state must have same id as resourceGroup name");
      }
      setupTool.getClusterManagementTool().setResourceGroupIdealState(clusterName,
                                                                      resourceGroupName,
                                                                      new IdealState(idealStateRecord));
      return 0;
    }
    else if (cmd.hasOption(addStat))
    {
      String clusterName = cmd.getOptionValues(addStat)[0];
      String statName = cmd.getOptionValues(addStat)[1];
      
      setupTool.getClusterManagementTool().addStat(clusterName, statName);
    }
    else if (cmd.hasOption(addAlert))
    {
      String clusterName = cmd.getOptionValues(addAlert)[0];
      String alertName = cmd.getOptionValues(addAlert)[1];
      
      setupTool.getClusterManagementTool().addAlert(clusterName, alertName);
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
    if (args.length == 1 && args[0].equals("setup-test-cluster"))
    {
      System.out.println("By default setting up ");
      new ClusterSetup("localhost:2181").setupTestCluster("storage-integration-cluster");
      new ClusterSetup("localhost:2181").setupTestCluster("relay-integration-cluster");
      System.exit(0);
    }

    int ret = processCommandLineArgs(args);
    System.exit(ret);
  }
}
