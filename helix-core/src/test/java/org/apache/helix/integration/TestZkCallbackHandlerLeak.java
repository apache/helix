package org.apache.helix.integration;

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

import java.util.Date;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.ZkHelixTestManager;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.CallbackHandler;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCallbackHandlerLeak extends ZkUnitTestBase {
	
	@Test
	public void testCbHandlerLeakOnParticipantSessionExpiry() throws Exception
	{
	    // Logger.getRootLogger().setLevel(Level.INFO);
	    String className = TestHelper.getTestClassName();
	    String methodName = TestHelper.getTestMethodName();
	    String clusterName = className + "_" + methodName;
	    int n = 2;

	    System.out.println("START " + clusterName + " at "
	        + new Date(System.currentTimeMillis()));

	    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
	                            "localhost", // participant name prefix
	                            "TestDB", // resource name prefix
	                            1, // resources
	                            32, // partitions per resource
	                            n, // number of nodes
	                            2, // replicas
	                            "MasterSlave",
	                            true); // do rebalance
	    
	    
	    ClusterController controller =
	        new ClusterController(clusterName, "controller_0", ZK_ADDR);
	    controller.syncStart();

	    // start participants
	    MockParticipant[] participants = new MockParticipant[n];
	    for (int i = 0; i < n; i++)
	    {
	      String instanceName = "localhost_" + (12918 + i);

	      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
	      participants[i].syncStart();
	    }

	    boolean result =
	        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                 clusterName));
	    Assert.assertTrue(result);
	    ZkHelixTestManager controllerManager = controller.getManager();
	    ZkHelixTestManager participantManagerToExpire = (ZkHelixTestManager)participants[1].getManager();

	    // printHandlers(controllerManager);
	    // printHandlers(participantManagerToExpire);
	    int controllerHandlerNb = controllerManager.getHandlers().size();
	    int particHandlerNb = participantManagerToExpire.getHandlers().size();
	    Assert.assertEquals(controllerHandlerNb, 9, "HelixController should have 9 (5+2n) callback handlers for 2 (n) participant");
	    Assert.assertEquals(particHandlerNb, 2, "HelixParticipant should have 2 (msg+cur-state) callback handlers");
	    
	    // expire the session of participant
	    System.out.println("Expiring participant session...");
	    String oldSessionId = participantManagerToExpire.getSessionId();
	    
	    ZkTestHelper.expireSession(participantManagerToExpire.getZkClient());
	    String newSessionId = participantManagerToExpire.getSessionId();
	    System.out.println("Expried participant session. oldSessionId: " + oldSessionId + ", newSessionId: " + newSessionId);

	    result =
	        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                                   clusterName));
	    Assert.assertTrue(result);
	    // printHandlers(controllerManager);
	    // printHandlers(participantManagerToExpire);
	    int handlerNb = controllerManager.getHandlers().size();
	    Assert.assertEquals(handlerNb, controllerHandlerNb, "controller callback handlers should not increase after participant session expiry");
	    handlerNb = participantManagerToExpire.getHandlers().size();
	    Assert.assertEquals(handlerNb, particHandlerNb, "participant callback handlers should not increase after participant session expiry");

	    System.out.println("END " + clusterName + " at "
	            + new Date(System.currentTimeMillis()));
	}
	
	@Test
	public void testCbHandlerLeakOnControllerSessionExpiry() throws Exception
	{
	    // Logger.getRootLogger().setLevel(Level.INFO);
	    String className = TestHelper.getTestClassName();
	    String methodName = TestHelper.getTestMethodName();
	    String clusterName = className + "_" + methodName;
	    int n = 2;

	    System.out.println("START " + clusterName + " at "
	        + new Date(System.currentTimeMillis()));

	    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
	                            "localhost", // participant name prefix
	                            "TestDB", // resource name prefix
	                            1, // resources
	                            32, // partitions per resource
	                            n, // number of nodes
	                            2, // replicas
	                            "MasterSlave",
	                            true); // do rebalance
	    
	    
	    ClusterController controller =
	        new ClusterController(clusterName, "controller_0", ZK_ADDR);
	    controller.syncStart();

	    // start participants
	    MockParticipant[] participants = new MockParticipant[n];
	    for (int i = 0; i < n; i++)
	    {
	      String instanceName = "localhost_" + (12918 + i);

	      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
	      participants[i].syncStart();
	    }

	    boolean result =
	        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                 clusterName));
	    Assert.assertTrue(result);
	    ZkHelixTestManager controllerManager = controller.getManager();
	    ZkHelixTestManager participantManager = participants[0].getManager();

	    // printHandlers(controllerManager);
	    int controllerHandlerNb = controllerManager.getHandlers().size();
	    int particHandlerNb = participantManager.getHandlers().size();
	    Assert.assertEquals(controllerHandlerNb, 9, "HelixController should have 9 (5+2n) callback handlers for 2 (n) participant");
	    Assert.assertEquals(particHandlerNb, 2, "HelixParticipant should have 2 (msg+cur-state) callback handlers");


	    // expire controller
	    System.out.println("Expiring controller session...");
	    String oldSessionId = controllerManager.getSessionId();
	    
	    ZkTestHelper.expireSession(controllerManager.getZkClient());
	    String newSessionId = controllerManager.getSessionId();
	    System.out.println("Expired controller session. oldSessionId: " + oldSessionId + ", newSessionId: " + newSessionId);
	    
	    result =
	        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                                   clusterName));
	    Assert.assertTrue(result);
	    // printHandlers(controllerManager);
	    int handlerNb = controllerManager.getHandlers().size();
	    Assert.assertEquals(handlerNb, controllerHandlerNb, "controller callback handlers should not increase after participant session expiry");
	    handlerNb = participantManager.getHandlers().size();
	    Assert.assertEquals(handlerNb, particHandlerNb, "participant callback handlers should not increase after participant session expiry");


	    System.out.println("END " + clusterName + " at "
	            + new Date(System.currentTimeMillis()));
	}

	static void printHandlers(ZkHelixTestManager manager) 
	{
	    List<CallbackHandler> handlers = manager.getHandlers();
    	System.out.println("\n" + manager.getInstanceName() + " cb-handler#: " + handlers.size());
    	
	    for (int i = 0; i < handlers.size(); i++) {
	    	CallbackHandler handler = handlers.get(i);
	    	String path = handler.getPath();
	    	System.out.println(path.substring(manager.getClusterName().length() + 1) + ": " + handler.getListener());
	    }
    }
}
