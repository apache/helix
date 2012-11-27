package org.apache.helix.integration;

import java.util.Date;

import org.apache.helix.TestHelper;
import org.apache.helix.mock.controller.ClusterController;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

// test auto-rebalance ideal state mode
// change ideal state when state transitions are in progress
public class TestAutoRebalanceDuringStart extends ZkIntegrationTestBase {
    @Test
    public void test() throws Exception {
	// Logger.getRootLogger().setLevel(Level.INFO);
	String className = TestHelper.getTestClassName();
	String methodName = TestHelper.getTestMethodName();
	String clusterName = className + "_" + methodName;
	final int n = 3;

	System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

	MockParticipant[] participants = new MockParticipant[n];

	TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
	        "localhost", // participant name prefix
	        "TestDB", // resource name prefix
	        1, // resources
	        4, // partitions per resource
	        n, // number of nodes
	        3, // replicas
	        "MasterSlave", 
	        IdealStateModeProperty.AUTO_REBALANCE, 
	        true); // do rebalance

	// start controller
	ClusterController controller = new ClusterController(clusterName, "controller_0", ZK_ADDR);
	controller.syncStart();

	// start participants
	for (int i = 0; i < n; i++) {
	    String instanceName = "localhost_" + (12918 + i);

	    participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR,
		    new MockParticipant.SleepTransition(1000));
	    participants[i].syncStart();
	    Thread.sleep(300);
	}
	boolean result = ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(
	        ZK_ADDR, clusterName));
	Assert.assertTrue(result);

	// clean up
	controller.syncStop();
	Thread.sleep(1000);  // wait for all zk callbacks done
	for (int i = 0; i < n; i++) {
	    participants[i].syncStop();
	}

	System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

    }

}
