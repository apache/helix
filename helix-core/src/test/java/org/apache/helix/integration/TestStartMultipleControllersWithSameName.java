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

import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStartMultipleControllersWithSameName extends ZkIntegrationTestBase {
    @Test
    public void test() throws Exception {
	Logger.getRootLogger().setLevel(Level.WARN);
	String className = TestHelper.getTestClassName();
	String methodName = TestHelper.getTestMethodName();
	String clusterName = className + "_" + methodName;
	final int n = 3;

	System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

	TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
	        "localhost", // participant name prefix
	        "TestDB", // resource name prefix
	        1, // resources
	        10, // partitions per resource
	        n, // number of nodes
	        1, // replicas
	        "OnlineOffline", RebalanceMode.FULL_AUTO, true); // do
									       // rebalance

	// start controller
	ClusterController[] controllers = new ClusterController[4];
	for (int i = 0; i < 4; i++) {
	    controllers[i] = new ClusterController(clusterName, "controller_0", ZK_ADDR);
	    controllers[i].start();
	}

	Thread.sleep(500); // wait leader election finishes
	String liPath = PropertyPathConfig.getPath(PropertyType.LIVEINSTANCES, clusterName);
	int listenerNb = ZkTestHelper.numberOfListeners(ZK_ADDR, liPath);
	// System.out.println("listenerNb: " + listenerNb);
	Assert.assertEquals(listenerNb, 1, "Only one controller should succeed in becoming leader");
	

	// clean up
	for (int i = 0; i < 4; i++) {
	    controllers[i].syncStop();
	    Thread.sleep(1000); // wait for all zk callbacks done
	}

	System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

    }

}
