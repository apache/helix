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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBatchMessage extends ZkIntegrationTestBase
{
  class TestZkChildListener implements IZkChildListener
  {
    int _maxNbOfChilds = 0;
    
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception
    {
      System.out.println(parentPath + " has " + currentChilds.size() + " messages");
      if (currentChilds.size() > _maxNbOfChilds)
      {
        _maxNbOfChilds = currentChilds.size();
      }
    }
    
  }
  
  @Test
  public void testBasic() throws Exception
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
    
    // enable batch message
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // registry a message listener so we know how many message generated
    TestZkChildListener listener = new TestZkChildListener();
    _gZkClient.subscribeChildChanges(keyBuilder.messages("localhost_12918").getPath(), listener);

    
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
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    Assert.assertTrue(listener._maxNbOfChilds <= 2, "Should get no more than 2 messages (O->S and S->M)");
    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < n; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
  
  // a non-batch-message run followed by a batch-message-enabled run
  @Test
  public void testChangeBatchMessageMode() throws Exception
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
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    
    // stop all participants
    Thread.sleep(1000);
    for (int i = 0; i < n; i++)
    {
      participants[i].syncStop();
    }
    
    // enable batch message
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // registry a message listener so we know how many message generated
    TestZkChildListener listener = new TestZkChildListener();
    _gZkClient.subscribeChildChanges(keyBuilder.messages("localhost_12918").getPath(), listener);

    // restart all participants
    for (int i = 0; i < n; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);
      participants[i].syncStart();
    }
    
    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);
    Assert.assertTrue(listener._maxNbOfChilds <= 2, "Should get no more than 2 messages (O->S and S->M)");

    
    // clean up
    // wait for all zk callbacks done
    Thread.sleep(1000);
    controller.syncStop();
    for (int i = 0; i < n; i++)
    {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));
  }
  
  @Test
  public void testSubMsgExecutionFail() throws Exception
  {
	    String className = TestHelper.getTestClassName();
	    String methodName = TestHelper.getTestMethodName();
	    String clusterName = className + "_" + methodName;
	    
	    final int n = 5;
	    MockParticipant[] participants = new MockParticipant[n];

	    System.out.println("START " + clusterName + " at "
	        + new Date(System.currentTimeMillis()));
//	    ZKHelixAdmin tool = new ZKHelixAdmin(_gZkClient);

	    TestHelper.setupCluster(clusterName,
	                            ZK_ADDR,
	                            12918,
	                            "localhost",
	                            "TestDB",
	                            1,	// resource#
	                            6,	// partition#
	                            n,	// nodes#
	                            3,  // replicas#
	                            "MasterSlave",
	                            true);

	    // enable batch message
	    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
	    Builder keyBuilder = accessor.keyBuilder();
	    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
	    idealState.setBatchMessageMode(true);
	    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

	    TestHelper.startController(clusterName,
	                               "controller_0",
	                               ZK_ADDR,
	                               HelixControllerMain.STANDALONE);
	    for (int i = 0; i < n; i++)
	    {
	      String instanceName = "localhost_" + (12918 + i);

	      if (i == 1)
	      {
	        Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>();
	        errPartitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_4"));
	        participants[i] =
	            new MockParticipant(clusterName,
	                                instanceName,
	                                ZK_ADDR,
	                                new ErrTransition(errPartitions));
	      }
	      else
	      {
	        participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR);
	      }
	      participants[i].syncStart();
	    }

	    Map<String, Map<String, String>> errStates =
	        new HashMap<String, Map<String, String>>();
	    errStates.put("TestDB0", new HashMap<String, String>());
	    errStates.get("TestDB0").put("TestDB0_4", "localhost_12919");
	    boolean result =
	        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
	                                                                                          clusterName,
	                                                                                          errStates));
	    Assert.assertTrue(result);

	    Map<String, Set<String>> errorStateMap = new HashMap<String, Set<String>>();
	    errorStateMap.put("TestDB0_4", TestHelper.setOf("localhost_12919"));

	    // verify "TestDB0_4", "localhost_12919" is in ERROR state
	    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");
	    
	    System.out.println("END " + clusterName + " at "
	            + new Date(System.currentTimeMillis()));

  }
}
