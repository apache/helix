package com.linkedin.helix.integration;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestSwapInstance extends ZkStandAloneCMTestBase
{
  @Test
  public void TestSwap() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    HelixDataAccessor helixAccessor = manager.getHelixDataAccessor();
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 64, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", _replica);
    
    
    ZNRecord idealStateOld1 = new ZNRecord("TestDB");
    ZNRecord idealStateOld2 = new ZNRecord("MyDB");
    
    IdealState is1 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("TestDB"));
    idealStateOld1.merge(is1.getRecord());
    

    IdealState is2 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("MyDB"));
    idealStateOld2.merge(is2.getRecord());
    
    String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + 0);
    ZKHelixAdmin tool = new ZKHelixAdmin(_zkClient);
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceName, false);

    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    
    String instanceName2 = PARTICIPANT_PREFIX + "_" + (START_PORT + 444);
    _setupTool.addInstanceToCluster(CLUSTER_NAME, instanceName2);
    
    boolean exception = false;
    try
    {
      _setupTool.swapInstance(CLUSTER_NAME, instanceName, instanceName2);
    }
    catch(Exception e)
    {
      exception = true;
    }
    Assert.assertTrue(exception);
    
    _startCMResultMap.get(instanceName)._manager.disconnect();
    _startCMResultMap.get(instanceName)._thread.interrupt();
    Thread.sleep(1000);
    
    exception = false;
    try
    {
      _setupTool.swapInstance(CLUSTER_NAME, instanceName, instanceName2);
    }
    catch(Exception e)
    {
      e.printStackTrace();
      exception = true;
    }
    Assert.assertFalse(exception);
    StartCMResult result2 =
        TestHelper.startDummyProcess(ZK_ADDR, CLUSTER_NAME, instanceName2);
    _startCMResultMap.put(instanceName2, result2);
    
    result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    
    is1 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("TestDB"));
    
    is2 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("MyDB"));
    
    for(String key : idealStateOld1.getMapFields().keySet())
    {
      for(String host : idealStateOld1.getMapField(key).keySet())
      {
        if(host.equals(instanceName))
        {
          Assert.assertTrue(
          idealStateOld1.getMapField(key).get(instanceName).equals(
          is1.getRecord().getMapField(key).get(instanceName2)));
        }
        else
        {
          Assert.assertTrue(
              idealStateOld1.getMapField(key).get(host).equals(
              is1.getRecord().getMapField(key).get(host)));
        }
      }
    }
    
    for(String key : idealStateOld1.getListFields().keySet())
    {
      Assert.assertEquals(idealStateOld1.getListField(key).size() , is1.getRecord().getListField(key).size());
      for(int i = 0; i < idealStateOld1.getListField(key).size(); i++)
      {
        String host = idealStateOld1.getListField(key).get(i);
        String newHost = is1.getRecord().getListField(key).get(i);
        if(host.equals(instanceName))
        {
          Assert.assertTrue(
              newHost.equals(instanceName2));
        }
        else
        {
          //System.out.println(key + " " + i+ " " + host + " "+newHost);
          //System.out.println(idealStateOld1.getListField(key));
          //System.out.println(is1.getRecord().getListField(key));
          
          Assert.assertTrue(host.equals(newHost));
        }
      }
    }
  }
}
