package com.linkedin.helix.manager.file;

import java.util.List;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.MockListener;
import com.linkedin.helix.mock.storage.DummyProcess;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.InstanceConfig;
import com.linkedin.helix.store.PropertyJsonComparator;
import com.linkedin.helix.store.PropertyJsonSerializer;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.store.PropertyStoreException;
import com.linkedin.helix.store.file.FilePropertyStore;

public class UnitTestDynFileClusterMgr
{
  final String className = "UnitTestDynFileClusterMgr";
  final String _rootNamespace = "/tmp/" + className;
  final String instanceName = "localhost_12918";

  FilePropertyStore<ZNRecord> _store;
  HelixAdmin _mgmtTool;

  @BeforeClass
  public void beforeClass()
  {
    _store = new FilePropertyStore<ZNRecord>(new PropertyJsonSerializer<ZNRecord>(ZNRecord.class),
        _rootNamespace, new PropertyJsonComparator<ZNRecord>(ZNRecord.class));
    try
    {
      _store.removeRootNamespace();
    } catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    _mgmtTool = new FileHelixAdmin(_store);
  }

  @Test()
  public void testBasic() throws PropertyStoreException
  {
    final String clusterName = className + "_basic";
    String controllerName = "controller_0";
    DynamicFileHelixManager controller = new DynamicFileHelixManager(clusterName,
        controllerName, InstanceType.CONTROLLER, _store);

    _mgmtTool.addCluster(clusterName, true);
    _mgmtTool.addInstance(clusterName, new InstanceConfig(instanceName));

    DynamicFileHelixManager participant = new DynamicFileHelixManager(clusterName,
        instanceName, InstanceType.PARTICIPANT, _store);

    AssertJUnit.assertEquals(instanceName, participant.getInstanceName());

    controller.disconnect();
    AssertJUnit.assertFalse(controller.isConnected());
    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());

    String sessionId = controller.getSessionId();
    // AssertJUnit.assertEquals(DynamicFileClusterManager._sessionId,
    // sessionId);
    AssertJUnit.assertEquals(clusterName, controller.getClusterName());
    AssertJUnit.assertEquals(0, controller.getLastNotificationTime());
    AssertJUnit.assertEquals(InstanceType.CONTROLLER, controller.getInstanceType());

    // AssertJUnit.assertNull(controller.getPropertyStore());
    PropertyStore<ZNRecord> propertyStore = controller.getPropertyStore();
    AssertJUnit.assertNotNull(propertyStore);
    propertyStore.setProperty("testKey", new ZNRecord("testValue"));
    ZNRecord record = propertyStore.getProperty("testKey");
    Assert.assertEquals(record.getId(), "testValue");

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
    } catch (HelixException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    // test message service
    ClusterMessagingService msgService = controller.getMessagingService();

    // test file management tool
    HelixAdmin tool = controller.getClusterManagmentTool();

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
      tool.getResourcesInCluster(clusterName);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.addResource(clusterName, "resource", 10, "MasterSlave",
          IdealStateModeProperty.AUTO.toString());
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
      tool.getResourceExternalView(clusterName, "resource");
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      tool.enableInstance(clusterName, "resource", false);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    tool.addCluster(clusterName, true);
    tool.addResource(clusterName, "resource", 10, "MasterSlave");
    InstanceConfig config = new InstanceConfig("nodeConfig");
    tool.addInstance(clusterName, config);
    List<String> instances = tool.getInstancesInCluster(clusterName);
    AssertJUnit.assertEquals(1, instances.size());
    tool.dropInstance(clusterName, config);

    IdealState idealState = new IdealState("idealState");
    tool.setResourceIdealState(clusterName, "resource", idealState);
    idealState = tool.getResourceIdealState(clusterName, "resource");
    AssertJUnit.assertEquals(idealState.getId(), "idealState");

    tool.dropResource(clusterName, "resource");
    _store.stop();
  }

  @Test
  public void testStartInstanceBeforeAdd()
  {
    final String clusterName = className + "_startInsB4Add";
    _mgmtTool.addCluster(clusterName, true);

    try
    {
      new DummyProcess(null, clusterName, instanceName, "dynamic-file", null, 0, _store).start();
      Assert.fail("Should fail since instance is not configured");
    } catch (HelixException e)
    {
      // OK
    } catch (Exception e)
    {
      Assert.fail("Should not fail on exceptions other than ClusterManagerException");
    }
  }
}
