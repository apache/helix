package com.linkedin.clustermanager.agent.file;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.ClusterManagementService;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.ClusterMessagingService;
import com.linkedin.clustermanager.InstanceType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.agent.MockListener;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class TestDynamicFileCM
{
  @Test(groups = { "unitTest" })
  public void testDynmaicFileCM()
  {
    String clusterName = "TestDynamicFileCM";
    String rootNamespace = "/tmp/" + clusterName;
    String controllerName = "controller_0";
    String instanceName = "localhost_8900";
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    FilePropertyStore<ZNRecord> store = new FilePropertyStore<ZNRecord>(serializer, rootNamespace, comparator);
    try
    {
      store.removeRootNamespace();
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ClusterDataAccessor accessor = new FileBasedDataAccessor(store, clusterName);
    DynamicFileClusterManager controller = new DynamicFileClusterManager(clusterName, controllerName,
                                    InstanceType.CONTROLLER, accessor);
    
    DynamicFileClusterManager participant = new DynamicFileClusterManager(clusterName, instanceName,
                                    InstanceType.PARTICIPANT, accessor);

    AssertJUnit.assertEquals(instanceName, participant.getInstanceName());
    
    controller.disconnect();
    AssertJUnit.assertFalse(controller.isConnected());
    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());
    
    String sessionId = controller.getSessionId();
    AssertJUnit.assertEquals(DynamicFileClusterManager._sessionId, sessionId);
    AssertJUnit.assertEquals(clusterName, controller.getClusterName());
    AssertJUnit.assertEquals(0, controller.getLastNotificationTime());
    AssertJUnit.assertEquals(InstanceType.CONTROLLER, controller.getInstanceType());
    AssertJUnit.assertNull(controller.getPropertyStore());
    AssertJUnit.assertNull(controller.getHealthReportCollector());
    
    MockListener controllerListener = new MockListener();
    controllerListener.reset();
    
    controller.addIdealStateChangeListener(controllerListener);
    AssertJUnit.assertTrue(controllerListener.isIdealStateChangeListenerInvoked);
    
    controller.addLiveInstanceChangeListener(controllerListener);
    AssertJUnit.assertTrue(controllerListener.isLiveInstanceChangeListenerInvoked);
    
    controller.addCurrentStateChangeListener(controllerListener, controllerName, sessionId);
    AssertJUnit.assertTrue(controllerListener.isCurrentStateChangeListenerInvoked);
    
    boolean exceptionCaught = false;
    try
    {
      controller.addConfigChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      controller.addExternalViewChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      controller.addControllerListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    AssertJUnit.assertFalse(controller.removeListener(controllerListener));
    
    exceptionCaught = false;
    try
    {
      controller.addIdealStateChangeListener(null);
    } catch (ClusterManagerException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    // test message service
    ClusterMessagingService msgService = controller.getMessagingService();
    
    // test file management tool
    ClusterManagementService tool = controller.getClusterManagmentTool();
    
    exceptionCaught = false;
    try
    {
      tool.getClusters();
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      tool.getResourceGroupsInCluster(clusterName);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.addResourceGroup(clusterName, "resourceGroup", 10, "MasterSlave", 
                            IdealStateConfigProperty.AUTO.toString());
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.getStateModelDefs(clusterName);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.getInstanceConfig(clusterName, instanceName);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      tool.getStateModelDef(clusterName, "MasterSlave");
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.getResourceGroupExternalView(clusterName, "resourceGroup");
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    exceptionCaught = false;
    try
    {
      tool.enableInstance(clusterName, "resourceGroup", false);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    tool.addCluster(clusterName, true);
    tool.addResourceGroup(clusterName, "resourceGroup", 10, "MasterSlave");
    ZNRecord nodeConfig = new ZNRecord("nodeConfig");
    tool.addInstance(clusterName, nodeConfig);
    List<String> instances = tool.getInstancesInCluster(clusterName);
    AssertJUnit.assertEquals(1, instances.size());
    tool.dropInstance(clusterName, nodeConfig);
    
    ZNRecord isRecord = new ZNRecord("idealState");
    tool.setResourceGroupIdealState(clusterName, "resourceGroup", isRecord);
    isRecord = tool.getResourceGroupIdealState(clusterName, "resourceGroup");
    AssertJUnit.assertEquals(isRecord.getId(), "idealState");
   
    tool.dropResourceGroup(clusterName, "resourceGroup");
    store.stop();
  }
}
