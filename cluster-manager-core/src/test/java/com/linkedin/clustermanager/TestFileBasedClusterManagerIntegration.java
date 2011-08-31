package com.linkedin.clustermanager;

import java.util.Date;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.ClusterPropertyType;
import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.ClusterDataAccessor.InstancePropertyType;
import com.linkedin.clustermanager.agent.file.FileBasedDataAccessor;
import com.linkedin.clustermanager.agent.file.FileClusterManagementTool;
import com.linkedin.clustermanager.controller.GenericClusterController;
import com.linkedin.clustermanager.mock.storage.DummyProcess;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.file.FilePropertyStore;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestFileBasedClusterManagerIntegration
{
  private static Logger LOG = Logger.getLogger(TestFileBasedClusterManagerIntegration.class);
  private final String _clusterName = "ESPRESSO_STORAGE";
  private final String _stateModelRefName = "MasterSlave";
  private final int _numNodes = 3;
  private FileClusterManagementTool _mgmtTool;
  
  @Test
  public void testInvocation() 
  throws Exception
  {
    LOG.info("RUN " + new Date(System.currentTimeMillis()));
    
    String rootPath = "/tmp/testFileCMIntegration";
    String filePath = rootPath + "/" + _clusterName;
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    FilePropertyStore<ZNRecord> store = new FilePropertyStore<ZNRecord>(serializer, rootPath, 
        comparator);

    FileBasedDataAccessor accessor = new FileBasedDataAccessor(store, _clusterName);
    
    // generate the cluster view file 
    _mgmtTool = new FileClusterManagementTool(store);    
    _mgmtTool.addCluster(_clusterName, true);
    _mgmtTool.addResourceGroup(_clusterName, "TestDB", 5, _stateModelRefName);
    for (int i = 0; i < _numNodes; i++)
    {
      addNodeToCluster(_clusterName, "localhost", 8900 + i);
    }
    rebalanceStorageCluster(_clusterName, "TestDB", 0);
    
    // start dummy storage node
    for (int i = 0; i < _numNodes; i++)
    {
      DummyProcess process = new DummyProcess(null, _clusterName, "localhost_" + (8900 + i), filePath, 0, accessor);
      process.start();
    }
    Thread.sleep(1000);
    
    
    // start cluster manager controller
    ClusterManager manager = ClusterManagerFactory.getFileBasedManagerForController(_clusterName, filePath, accessor);

    GenericClusterController controller = new GenericClusterController();
    // manager.addConfigChangeListener(controller);
    manager.addLiveInstanceChangeListener(controller);
    manager.addIdealStateChangeListener(controller);
    // manager.addExternalViewChangeListener(controller);

    manager.connect();
    
    Thread.sleep(10000);
    // verify current state
    ZNRecord idealStates = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, "TestDB");
    ZNRecord curStates =  accessor.getInstanceProperty("localhost_8900", InstancePropertyType.CURRENTSTATES, 
                                              manager.getSessionId(), "TestDB");
    boolean result = verifyCurStateAndIdealState(curStates, idealStates,"localhost_8900", "TestDB");
    Assert.assertTrue(result);
    
    
    // add a new db
    _mgmtTool.addResourceGroup(_clusterName, "MyDB", 6, _stateModelRefName);
    rebalanceStorageCluster(_clusterName, "MyDB", 0);
    
    Thread.sleep(10000);
    // verify current state
    idealStates = accessor.getClusterProperty(ClusterPropertyType.IDEALSTATES, "MyDB");
    curStates =  accessor.getInstanceProperty("localhost_8900", InstancePropertyType.CURRENTSTATES, 
                                              manager.getSessionId(), "MyDB");
    result = verifyCurStateAndIdealState(curStates, idealStates,"localhost_8900", "MyDB");
    Assert.assertTrue(result);
 
    LOG.info("END " + new Date(System.currentTimeMillis()));
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
    _mgmtTool.addNode(_clusterName, nodeConfig);
  }
  
  private void rebalanceStorageCluster(String clusterName, String resourceGroupName, int replica)
  {
    List<String> nodeNames = _mgmtTool.getNodeNamesInCluster(clusterName);

    ZNRecord idealState = _mgmtTool.getResourceGroupIdealState(clusterName, resourceGroupName);
    int partitions = Integer.parseInt(idealState.getSimpleField("partitions"));

    ZNRecord newIdealState 
      = IdealStateCalculatorForStorageNode.calculateIdealState(nodeNames, partitions, replica, resourceGroupName);
    
    newIdealState.merge(idealState);
    _mgmtTool.setResourceGroupIdealState(clusterName, resourceGroupName, newIdealState);
  }
  
  private boolean verifyCurStateAndIdealState(ZNRecord curStates, ZNRecord idealStates, 
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
  
  /**
  public Thread startDummyProcess(final String clusterName, final String instanceName)
  {
    Thread thread = new Thread(new Runnable()
    )
  }
  **/
}
