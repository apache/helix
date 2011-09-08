package com.linkedin.clustermanager;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.controller.GenericClusterController;
import com.linkedin.clustermanager.mock.storage.DummyProcess;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.file.FilePropertyStore;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

// start once for all tests that use dynamic-file-based cluster manager
public class FileClusterManagerHandler
{
  private static Logger logger = Logger.getLogger(FileClusterManagerHandler.class);
  protected static final String storageCluster = "ESPRESSO_STORAGE";
  private static final String testDB = "TestDB";
  protected static final String stateModel = "MasterSlave";
  protected static final int storageNodeNr = 5;
  protected static final int startPort = 12918;
  
  private static final String rootPath = "/tmp/FileClusterManagerHandler";
  private static final PropertyJsonSerializer<ZNRecord> serializer 
      = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
  private static final PropertyJsonComparator<ZNRecord> comparator 
      = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
  private static final FilePropertyStore<ZNRecord> store 
      = new FilePropertyStore<ZNRecord>(serializer, rootPath, comparator);
  protected static final FileBasedDataAccessor accessor 
      = new FileBasedDataAccessor(store, storageCluster);
  protected static final ClusterManager manager 
      = ClusterManagerFactory.getFileBasedManagerForController(storageCluster, accessor);
  protected static final ClusterManagementService mgmtTool = manager.getClusterManagmentTool();

  static
  {
    // setup cluster
    mgmtTool.addCluster(storageCluster, true);
    mgmtTool.addResourceGroup(storageCluster, testDB, 20, stateModel);
    for (int i = 0; i < storageNodeNr; i++)
    {
      addNodeToCluster(storageCluster, "localhost", startPort + i);
    }
    rebalanceStorageCluster(storageCluster, testDB, 3);
    
    // start dummy storage nodes
    for (int i = 0; i < storageNodeNr; i++)
    {
      DummyProcess process = new DummyProcess(null, storageCluster, "localhost_" + (startPort + i), 
                                              null, 0, accessor);
      try
      {
        process.start();
      }
      catch (Exception e)
      {
        logger.error("fail to start dummy stroage node for file-based cluster-manager" + 
            "\nexception:" + e);
      }
    }

    // start cluster manager controller
    GenericClusterController controller = new GenericClusterController();
    try
    {
      // manager.addConfigChangeListener(controller);
      manager.addLiveInstanceChangeListener(controller);
      manager.addIdealStateChangeListener(controller);
      // manager.addExternalViewChangeListener(controller);
      manager.connect();
    }
    catch(Exception e)
    {
      logger.error("fail to start file-based cluster-manager controller" + 
          "\nexception:" + e);
    }

    // wait until all transitions finished
    // TODO replace sleep with verifier
    try
    {
      Thread.sleep(10000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // verify current state
    ZNRecord idealStates = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, testDB);
    for (int i = 0; i < storageNodeNr; i++)
    {
      ZNRecord curStates =  accessor.getInstanceProperty("localhost_" + (startPort + i), 
                InstancePropertyType.CURRENTSTATES, manager.getSessionId(), testDB);
      boolean result = verifyCurStateAndIdealState(curStates, idealStates, 
                                   "localhost_" + (startPort + i), testDB);
      Assert.assertTrue(result);
    }

  }
  
  private static void addNodeToCluster(String clusterName, String host, int port)
  {
    // TODO use ClusterSetup
    ZNRecord nodeConfig = new ZNRecord();
    String nodeId = host + "_" + port;
    nodeConfig.setId(nodeId);
    nodeConfig.setSimpleField(InstanceConfigProperty.HOST.toString(), host);
    nodeConfig.setSimpleField(InstanceConfigProperty.PORT.toString(), Integer.toString(port));
    nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(), Boolean.toString(true));
    mgmtTool.addInstance(storageCluster, nodeConfig);
  }
  
  protected static void rebalanceStorageCluster(String clusterName, String resourceGroupName, int replica)
  {
    List<String> nodeNames = mgmtTool.getInstancesInCluster(clusterName);

    ZNRecord idealState = mgmtTool.getResourceGroupIdealState(clusterName, resourceGroupName);
    int partitions = Integer.parseInt(idealState.getSimpleField("partitions"));

    ZNRecord newIdealState = IdealStateCalculatorForStorageNode.calculateIdealState(nodeNames, 
                                   partitions, replica, resourceGroupName, "MASTER", "SLAVE");
    
    newIdealState.merge(idealState);
    mgmtTool.setResourceGroupIdealState(clusterName, resourceGroupName, newIdealState);
  }
  
  protected static boolean verifyCurStateAndIdealState(ZNRecord curStates, ZNRecord idealStates, 
     String instanceName, String resourceGroupName)
  {
    for (Map.Entry<String, List<String>> idealStateEntry : idealStates.getListFields().entrySet())
    {
      String dbTablet = idealStateEntry.getKey();
      String instance = idealStateEntry.getValue().get(0);
      if (!instance.equals(instanceName))
        continue;
      
      for (Map.Entry<String, Map<String, String>> curStateEntry : curStates.getMapFields().entrySet())
      {
        if (dbTablet.equals(curStateEntry.getKey()))
        {
          String curState = curStateEntry.getValue().get("CURRENT_STATE");
          if (curState == null || !curState.equals("MASTER"))
            return false;
        }
            
      }
    }
    return true;
  }
  
  protected void verifyEmptyCurrentState(String instanceName, String resourceGroup)
  {
    List<String> subPaths = accessor.getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);
    
    for (String prevSession : subPaths)
    {
      if(store.exists(prevSession + "/" + resourceGroup))
      {
        String prevSessionId = prevSession.substring(prevSession.lastIndexOf('/') + 1); 
        ZNRecord prevCurrentState = accessor.getInstanceProperty(instanceName, 
                  InstancePropertyType.CURRENTSTATES, prevSessionId, resourceGroup);
        AssertJUnit.assertTrue(prevCurrentState.getMapFields().size() == 0);
      }
    }
  }
  
  /**
  @Test
  public void testFileClusterManagerHandler()
  {
    logger.info("Run at " + new Date(System.currentTimeMillis()));
  }
  **/
}
