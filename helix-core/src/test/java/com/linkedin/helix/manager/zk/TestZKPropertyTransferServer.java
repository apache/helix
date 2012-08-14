package com.linkedin.helix.manager.zk;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixDataAccessor;
import com.linkedin.helix.TestHelper;
import com.linkedin.helix.TestHelper.StartCMResult;
import com.linkedin.helix.controller.HelixControllerMain;
import com.linkedin.helix.controller.restlet.ZKPropertyTransferServer;
import com.linkedin.helix.integration.ZkStandAloneCMTestBaseWithPropertyServerCheck;

public class TestZKPropertyTransferServer extends ZkStandAloneCMTestBaseWithPropertyServerCheck
{
  private static Logger LOG =
      Logger.getLogger(TestZKPropertyTransferServer.class);

  @Test
  public void TestControllerChange() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    _startCMResultMap.get(controllerName)._manager.disconnect();
    
    Thread.sleep(1000);
    
    // kill controller, participant should not know about the svc url
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      HelixDataAccessor accessor = _startCMResultMap.get(instanceName)._manager.getHelixDataAccessor();
      ZKHelixDataAccessor zkAccessor = (ZKHelixDataAccessor) accessor;
      Assert.assertTrue(zkAccessor._zkPropertyTransferSvcUrl == null || zkAccessor._zkPropertyTransferSvcUrl.equals(""));
    }
    _startCMResultMap.get(controllerName)._thread.interrupt();
    _startCMResultMap.remove(controllerName);
    
    StartCMResult startResult =
        TestHelper.startController(CLUSTER_NAME,
                                   controllerName,
                                   ZK_ADDR,
                                   HelixControllerMain.STANDALONE);
    _startCMResultMap.put(controllerName, startResult);
    
    Thread.sleep(1000);
    
    // create controller again, the svc url is notified to the participants
    for (int i = 0; i < NODE_NR; i++)
    {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      HelixDataAccessor accessor = _startCMResultMap.get(instanceName)._manager.getHelixDataAccessor();
      ZKHelixDataAccessor zkAccessor = (ZKHelixDataAccessor) accessor;
      Assert.assertTrue(zkAccessor._zkPropertyTransferSvcUrl.equals(ZKPropertyTransferServer.getInstance().getWebserviceUrl()));
    }
  }
  

}
