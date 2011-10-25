package com.linkedin.clustermanager;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor.InstanceConfigProperty;
import com.linkedin.clustermanager.agent.zk.ZKDataAccessor;
import com.linkedin.clustermanager.healthcheck.PerformanceHealthReportProvider;
import com.linkedin.clustermanager.tools.ClusterSetup;
import com.linkedin.clustermanager.util.CMUtil;
import com.linkedin.clustermanager.util.ZKClientPool;

public class TestClusterSetup extends ZkUnitTestBase 
{
	private static Logger LOG = Logger.getLogger(TestClusterSetup.class);

	protected static final String CLUSTER_NAME = "TestCluster";
	protected static final String TEST_DB = "TestDB";
	protected static final String INSTANCE_PREFIX = "instance:";
	protected static final String STATE_MODEL = "MasterSlave";
	protected static final String TEST_NODE = "testnode:1";
	
	ClusterSetup _clusterSetup;
	
	String instanceColonToUnderscoreFormat(String colonFormat) 
	{
		int lastPos = colonFormat.lastIndexOf(":");
		  if (lastPos <= 0)
		  {
			  String error = "Invalid storage Instance info format: " + colonFormat;
			  LOG.warn(error);
			  throw new ClusterManagerException(error);
		  }
		String host =colonFormat.substring(0, lastPos);
		String portStr = colonFormat.substring(lastPos + 1);
		return host+"_"+portStr;
	}
	
	private static String[] createArgs(String str)
	{
		String[] split = str.split("[ ]+");
		System.out.println(Arrays.toString(split));
		return split;
	}

	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_zkClient.deleteRecursive("/" + CLUSTER_NAME);
		_clusterSetup = new ClusterSetup(ZK_ADDR);
		_clusterSetup.addCluster(CLUSTER_NAME, true);
	}
	
	 @Test (groups = {"unitTest"})
	  public void testAddInstancesToCluster() throws Exception
	  {
		String instanceAddresses[] = new String [3];
		for (int i=0;i<3;i++) {
			String currInstance = INSTANCE_PREFIX+i;
			instanceAddresses[i] = currInstance;
		}
		String nextInstanceAddress = INSTANCE_PREFIX+3;
		
		_clusterSetup.addInstancesToCluster(CLUSTER_NAME, instanceAddresses);
		
		//verify instances
		for (String instance : instanceAddresses) {
			verifyInstance(CLUSTER_NAME, instanceColonToUnderscoreFormat(instance), true);
		}
		
		
		_clusterSetup.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
		verifyInstance(CLUSTER_NAME, instanceColonToUnderscoreFormat(nextInstanceAddress), true);
		//re-add
		boolean caughtException = false;
		try {
			_clusterSetup.addInstanceToCluster(CLUSTER_NAME, nextInstanceAddress);
		} catch (ClusterManagerException e) {
			caughtException = true;
		}
		AssertJUnit.assertTrue(caughtException);

		//bad instance format
		String badFormatInstance = "badinstance";
		caughtException = false;
		try {
			_clusterSetup.addInstanceToCluster(CLUSTER_NAME, badFormatInstance);
		} catch (ClusterManagerException e) {
			caughtException = true;
		}
		AssertJUnit.assertTrue(caughtException);
		
	  }
	 
	 @Test (groups = {"unitTest"})
	  public void testDisableDropInstancesFromCluster() throws Exception
	  {
		 testAddInstancesToCluster();
		 String instanceAddresses[] = new String [3];
		 for (int i=0;i<3;i++) {
			 String currInstance = INSTANCE_PREFIX+i;
			 instanceAddresses[i] = currInstance;
		 }
		 String nextInstanceAddress = INSTANCE_PREFIX+3;
		 
		 boolean caughtException = false;
		 //drop without disabling
		 try {
			 _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
		 } catch (ClusterManagerException e) {
			 caughtException = true;
		 }
		 AssertJUnit.assertTrue(caughtException);
		 
		 //disable
		 _clusterSetup.getClusterManagementTool().enableInstance(CLUSTER_NAME, 
				 instanceColonToUnderscoreFormat(nextInstanceAddress), false);
		 verifyEnabled(CLUSTER_NAME, instanceColonToUnderscoreFormat(nextInstanceAddress), false);
		 
		 //drop
		 _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
		 verifyInstance(CLUSTER_NAME, instanceColonToUnderscoreFormat(nextInstanceAddress), false);
		 
		 //re-drop
		 caughtException = false;
		 try {
			 _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, nextInstanceAddress);
		 } catch (ClusterManagerException e) {
			 caughtException = true;
		 }
		 AssertJUnit.assertTrue(caughtException);
		 /*
		 //drop a set
		 _clusterSetup.getClusterManagementTool().enableInstances(CLUSTER_NAME, instanceAddresses, false);
		 _clusterSetup.dropInstancesFromCluster(CLUSTER_NAME, instanceAddresses);
		*/

		 //bad format disable, drop
		 String badFormatInstance = "badinstance";
		 caughtException = false;
		 try {
			 _clusterSetup.getClusterManagementTool().enableInstance(CLUSTER_NAME, badFormatInstance, false);
		 } catch (ClusterManagerException e) {
			 caughtException = true;
		 }
		 AssertJUnit.assertTrue(caughtException);

		 caughtException = false;
		 try {
			 _clusterSetup.dropInstanceFromCluster(CLUSTER_NAME, badFormatInstance);
		 } catch (ClusterManagerException e) {
			 caughtException = true;
		 }
		 AssertJUnit.assertTrue(caughtException);
	  }
	 
	 @Test (groups = {"unitTest"})
	  public void testAddResourceGroup() throws Exception
	  {
		 _clusterSetup.addResourceGroupToCluster(CLUSTER_NAME, TEST_DB, 16, STATE_MODEL);
		 verifyResource(CLUSTER_NAME, TEST_DB, true);
	  }
	 
	 @Test (groups = {"unitTest"})
	  public void testRemoveResourceGroup() throws Exception
	  {
		 _clusterSetup.setupTestCluster(CLUSTER_NAME);
		 //testAddResourceGroup();
		 verifyResource(CLUSTER_NAME, TEST_DB, true);
		 _clusterSetup.dropResourceGroupToCluster(CLUSTER_NAME, TEST_DB);
		 verifyResource(CLUSTER_NAME, TEST_DB, false);
	  }
	 
	 
	 @Test (groups = {"unitTest"})
	  public void testRebalanceCluster() throws Exception
	  {
		 _clusterSetup.setupTestCluster(CLUSTER_NAME);
		 //testAddInstancesToCluster();
		 testAddResourceGroup();
		 _clusterSetup.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, 4);
		 verifyReplication(CLUSTER_NAME, TEST_DB, 4);
	  }
	 
	 /*
	 @Test (groups = {"unitTest"})
	  public void testPrintUsage() throws Exception
	  {
		 Options cliOptions = ClusterSetup.constructCommandLineOptions();
		 ClusterSetup.printUsage(null);
	  }
	  */
	 
	 
	 
	 @Test (groups = {"unitTest"})
	  public void testParseCommandLinesArgs() throws Exception
	  {
		// ClusterSetup
	     //   .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+ " help"));
		
		 //wipe ZK
		 _zkClient.deleteRecursive("/" + CLUSTER_NAME);
		 _clusterSetup = new ClusterSetup(ZK_ADDR);
		 
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addCluster "+CLUSTER_NAME));
		 
		 //wipe again
		 _zkClient.deleteRecursive("/" + CLUSTER_NAME);
		 _clusterSetup = new ClusterSetup(ZK_ADDR);
		 
		 _clusterSetup.setupTestCluster(CLUSTER_NAME);
		 
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addNode "+CLUSTER_NAME+" "+TEST_NODE));
		 verifyInstance(CLUSTER_NAME, instanceColonToUnderscoreFormat(TEST_NODE), true);
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addResourceGroup "+CLUSTER_NAME+" "+TEST_DB+" 4 "+STATE_MODEL));
		 verifyResource(CLUSTER_NAME, TEST_DB, true);
		// ClusterSetup
	     //   .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --addNode node-1"));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --enableInstance "+CLUSTER_NAME+" "+instanceColonToUnderscoreFormat(TEST_NODE)+ " true"));
		 verifyEnabled(CLUSTER_NAME, instanceColonToUnderscoreFormat(TEST_NODE), true);
		 
		 //TODO: verify list commands
		 /*
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listClusterInfo "+CLUSTER_NAME));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listClusters"));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listInstanceInfo "+CLUSTER_NAME+" "+instanceColonToUnderscoreFormat(TEST_NODE)));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listInstances "+CLUSTER_NAME));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listResourceGroupInfo "+CLUSTER_NAME+" "+TEST_DB));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listResourceGroups "+CLUSTER_NAME));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listStateModel "+CLUSTER_NAME+" "+STATE_MODEL));
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --listStateModels "+CLUSTER_NAME));
	      */
		 //ClusterSetup
	     //   .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --rebalance "+CLUSTER_NAME+" "+TEST_DB+" 1")); 
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --enableInstance "+CLUSTER_NAME+" "+instanceColonToUnderscoreFormat(TEST_NODE)+ " false"));
		 verifyEnabled(CLUSTER_NAME, instanceColonToUnderscoreFormat(TEST_NODE), false);
		 ClusterSetup
	        .processCommandLineArgs(createArgs("-zkSvr "+ZK_ADDR+" --dropNode "+CLUSTER_NAME+" "+TEST_NODE));
	  }
}
