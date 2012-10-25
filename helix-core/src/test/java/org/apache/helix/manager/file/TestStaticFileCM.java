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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.helix.ClusterView;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.MockListener;
import org.apache.helix.manager.file.StaticFileHelixManager;
import org.apache.helix.manager.file.StaticFileHelixManager.DBParam;
import org.apache.helix.tools.ClusterViewSerializer;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestStaticFileCM
{
  @Test()
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
      view = StaticFileHelixManager.generateStaticConfigClusterView(illegalNodesInfo, dbParams, 3);
    } catch (IllegalArgumentException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    String[] nodesInfo = {"localhost:8900", "localhost:8901", "localhost:8902"};
    view = StaticFileHelixManager.generateStaticConfigClusterView(nodesInfo, dbParams, 2);

    String configFile = "/tmp/" + clusterName;
    ClusterViewSerializer.serialize(view, new File(configFile));
    ClusterView restoredView = ClusterViewSerializer.deserialize(new File(configFile));
    // System.out.println(restoredView);
    // byte[] bytes = ClusterViewSerializer.serialize(restoredView);
    // System.out.println(new String(bytes));

    StaticFileHelixManager.verifyFileBasedClusterStates("localhost_8900",
                                       configFile, configFile);

    StaticFileHelixManager controller = new StaticFileHelixManager(clusterName, controllerName,
                                                     InstanceType.CONTROLLER, configFile);
    controller.disconnect();
    AssertJUnit.assertFalse(controller.isConnected());
    controller.connect();
    AssertJUnit.assertTrue(controller.isConnected());

    String sessionId = controller.getSessionId();
//    AssertJUnit.assertEquals(DynamicFileClusterManager._sessionId, sessionId);
    AssertJUnit.assertEquals(clusterName, controller.getClusterName());
    AssertJUnit.assertEquals(0, controller.getLastNotificationTime());
    AssertJUnit.assertEquals(InstanceType.CONTROLLER, controller.getInstanceType());
    AssertJUnit.assertNull(controller.getPropertyStore());
    AssertJUnit.assertNull(controller.getHealthReportCollector());
    AssertJUnit.assertEquals(controllerName, controller.getInstanceName());
    AssertJUnit.assertNull(controller.getClusterManagmentTool());
    AssertJUnit.assertNull(controller.getMessagingService());

    MockListener controllerListener = new MockListener();
    AssertJUnit.assertFalse(controller.removeListener(controllerListener));
    controllerListener.reset();

    controller.addIdealStateChangeListener(controllerListener);
    AssertJUnit.assertTrue(controllerListener.isIdealStateChangeListenerInvoked);

    controller.addMessageListener(controllerListener, "localhost_8900");
    AssertJUnit.assertTrue(controllerListener.isMessageListenerInvoked);

    exceptionCaught = false;
    try
    {
      controller.addLiveInstanceChangeListener(controllerListener);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
    try
    {
      controller.addCurrentStateChangeListener(controllerListener, "localhost_8900", sessionId);
    } catch (UnsupportedOperationException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    exceptionCaught = false;
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

  }

}
