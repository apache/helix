package org.apache.helix.manager.file;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.MockListener;
import org.apache.helix.manager.file.DynamicFileHelixManager;
import org.apache.helix.manager.file.FileHelixAdmin;
import org.apache.helix.mock.storage.DummyProcess;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStore;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.store.file.FilePropertyStore;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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
