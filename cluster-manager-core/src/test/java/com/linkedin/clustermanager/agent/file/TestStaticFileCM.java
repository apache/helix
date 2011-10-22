package com.linkedin.clustermanager.agent.file;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterView;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.agent.MockListener;
import com.linkedin.clustermanager.agent.file.FileBasedClusterManager.DBParam;
import com.linkedin.clustermanager.tools.ClusterViewSerializer;

public class TestStaticFileCM
{
  @Test(groups = { "unitTest" })
  public void testStaticFileCM()
  {
    final String clusterName = "TestSTaticFileCM";
    final String controllerName = "controller_0";
    
    ClusterView view;
    String[] illegalNodesInfo = {"localhost_8900", "localhost_8901"};
    List<DBParam> dbParams = new ArrayList<DBParam>();
    dbParams.add(new DBParam("TestDB0", 10));
    dbParams.add(new DBParam("TestDB1", 10));
    
    boolean exceptionCaught = false;
    try
    {
      view = FileBasedClusterManager.generateStaticConfigClusterView(illegalNodesInfo, dbParams, 3);
    } catch (IllegalArgumentException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
    String[] nodesInfo = {"localhost:8900", "localhost:8901", "localhost:8902"};
    view = FileBasedClusterManager.generateStaticConfigClusterView(nodesInfo, dbParams, 2);
    
    String configFile = "/tmp/" + clusterName;
    ClusterViewSerializer.serialize(view, new File(configFile));
    ClusterView restoredView = ClusterViewSerializer.deserialize(new File(configFile));
    // System.out.println(restoredView);
    // byte[] bytes = ClusterViewSerializer.serialize(restoredView);
    // System.out.println(new String(bytes));

    FileBasedClusterManager.verifyFileBasedClusterStates("localhost_8900",
                                       configFile, configFile);
    
    FileBasedClusterManager controller = new FileBasedClusterManager(clusterName, controllerName,
                                                     InstanceType.CONTROLLER, configFile);
    controller.disconnect();
    Assert.assertFalse(controller.isConnected());
    controller.connect();
    Assert.assertTrue(controller.isConnected());
    
    String sessionId = controller.getSessionId();
    Assert.assertEquals(DynamicFileClusterManager._sessionId, sessionId);
    Assert.assertEquals(clusterName, controller.getClusterName());
    Assert.assertEquals(0, controller.getLastNotificationTime());
    Assert.assertEquals(InstanceType.CONTROLLER, controller.getInstanceType());
    Assert.assertNull(controller.getPropertyStore());
    Assert.assertNull(controller.getHealthReportCollector());
    Assert.assertEquals(controllerName, controller.getInstanceName());
    Assert.assertNull(controller.getClusterManagmentTool());
    Assert.assertNull(controller.getMessagingService());
    
    MockListener controllerListener = new MockListener();
    Assert.assertFalse(controller.removeListener(controllerListener));
    controllerListener.reset();
    
    controller.addIdealStateChangeListener(controllerListener);
    Assert.assertTrue(controllerListener.isIdealStateChangeListenerInvoked);

    controller.addMessageListener(controllerListener, "localhost_8900");
    Assert.assertTrue(controllerListener.isMessageListenerInvoked);
    
    exceptionCaught = false;
    try
    {
      controller.addLiveInstanceChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      controller.addCurrentStateChangeListener(controllerListener, "localhost_8900", sessionId);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      controller.addConfigChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      controller.addExternalViewChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      controller.addControllerListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    Assert.assertTrue(exceptionCaught);

  }

}
