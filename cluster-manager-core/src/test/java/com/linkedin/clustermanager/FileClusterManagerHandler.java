package com.linkedin.clustermanager;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

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

/**
 * Base test for dynamic file-based cluster manager
 * 
 * @author zzhang
 *
 */
public class FileClusterManagerHandler
{
  private static Logger logger = Logger.getLogger(FileClusterManagerHandler.class);
  protected static final String CLUSTER_NAME = "ESPRESSO_STORAGE";
  private static final String TEST_DB = "TestDB";
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  private final String ROOT_PATH = "/tmp/" + getShortClassName();
  
  private static final PropertyJsonSerializer<ZNRecord> serializer 
      = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
  private static final PropertyJsonComparator<ZNRecord> comparator 
      = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
  
  private final FilePropertyStore<ZNRecord> _store 
      = new FilePropertyStore<ZNRecord>(serializer, ROOT_PATH, comparator);
  protected final FileBasedDataAccessor _accessor 
      = new FileBasedDataAccessor(_store, CLUSTER_NAME);
  protected final ClusterManager _manager 
      = ClusterManagerFactory.getFileBasedManagerForController(CLUSTER_NAME, _accessor);
  protected final ClusterManagementService _mgmtTool = _manager.getClusterManagmentTool();

  // static
  @BeforeClass
  public void beforeClass()
  {
    // setup cluster
    _mgmtTool.addCluster(CLUSTER_NAME, true);
    _mgmtTool.addResourceGroup(CLUSTER_NAME, TEST_DB, 20, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++)
    {
      addNodeToCluster(CLUSTER_NAME, "localhost", START_PORT + i);
    }
    rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 3);
    
    // start dummy storage nodes
    for (int i = 0; i < NODE_NR; i++)
    {
      DummyProcess process = new DummyProcess(null, CLUSTER_NAME, "localhost_" + (START_PORT + i), 
                                              null, 0, _accessor);
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
      _manager.addLiveInstanceChangeListener(controller);
      _manager.addIdealStateChangeListener(controller);
      // manager.addExternalViewChangeListener(controller);
      _manager.connect();
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
      Thread.sleep(15000);
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // verify current state
    ZNRecord idealStates = _accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, TEST_DB);
    for (int i = 0; i < NODE_NR; i++)
    {
      ZNRecord curStates =  _accessor.getInstanceProperty("localhost_" + (START_PORT + i), 
                InstancePropertyType.CURRENTSTATES, _manager.getSessionId(), TEST_DB);
      boolean result = verifyCurStateAndIdealState(curStates, idealStates, 
                                   "localhost_" + (START_PORT + i), TEST_DB);
      Assert.assertTrue(result);
    }

  }
  
  @AfterClass
  public void afterClass() throws Exception
  {
    logger.info("END shutting down file-based cluster managers at " + 
             new Date(System.currentTimeMillis()));
    
    // Thread.sleep(3000);
    _store.stop();
    logger.info("END at " + new Date(System.currentTimeMillis()));
    
  }

  private String getShortClassName()
  {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  private void addNodeToCluster(String clusterName, String host, int port)
  {
    // TODO use ClusterSetup
    ZNRecord nodeConfig = new ZNRecord();
    String nodeId = host + "_" + port;
    nodeConfig.setId(nodeId);
    nodeConfig.setSimpleField(InstanceConfigProperty.HOST.toString(), host);
    nodeConfig.setSimpleField(InstanceConfigProperty.PORT.toString(), Integer.toString(port));
    nodeConfig.setSimpleField(InstanceConfigProperty.ENABLED.toString(), Boolean.toString(true));
    _mgmtTool.addInstance(CLUSTER_NAME, nodeConfig);
  }
  
  protected void rebalanceStorageCluster(String clusterName, String resourceGroupName, int replica)
  {
    List<String> nodeNames = _mgmtTool.getInstancesInCluster(clusterName);

    ZNRecord idealState = _mgmtTool.getResourceGroupIdealState(clusterName, resourceGroupName);
    int partitions = Integer.parseInt(idealState.getSimpleField("partitions"));

    ZNRecord newIdealState = IdealStateCalculatorForStorageNode.calculateIdealState(nodeNames, 
                                   partitions, replica, resourceGroupName, "MASTER", "SLAVE");
    
    newIdealState.merge(idealState);
    _mgmtTool.setResourceGroupIdealState(clusterName, resourceGroupName, newIdealState);
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
    List<String> subPaths = _accessor.getInstancePropertySubPaths(instanceName, InstancePropertyType.CURRENTSTATES);
    
    for (String prevSession : subPaths)
    {
      if(_store.exists(prevSession + "/" + resourceGroup))
      {
        String prevSessionId = prevSession.substring(prevSession.lastIndexOf('/') + 1); 
        ZNRecord prevCurrentState = _accessor.getInstanceProperty(instanceName, 
                  InstancePropertyType.CURRENTSTATES, prevSessionId, resourceGroup);
        AssertJUnit.assertTrue(prevCurrentState.getMapFields().size() == 0);
      }
    }
  }
  
  // @Test
  public void testFileClusterManagerHandler()
  {
    logger.info("Dummy run at " + new Date(System.currentTimeMillis()));
  }
  
}
