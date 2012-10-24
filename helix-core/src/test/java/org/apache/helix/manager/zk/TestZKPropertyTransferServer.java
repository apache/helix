package org.apache.helix.manager.zk;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.StartCMResult;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.controller.restlet.ZKPropertyTransferServer;
import org.apache.helix.integration.ZkStandAloneCMTestBaseWithPropertyServerCheck;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


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
