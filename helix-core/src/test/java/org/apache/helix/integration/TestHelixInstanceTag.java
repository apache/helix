package org.apache.helix.integration;

import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixInstanceTag extends ZkStandAloneCMTestBase
{
  @Test
  public void testInstanceTag() throws Exception
  {
    String controllerName = CONTROLLER_PREFIX + "_0";
    HelixManager manager = _startCMResultMap.get(controllerName)._manager;
    HelixDataAccessor accessor = manager.getHelixDataAccessor();
    
    String DB2 = "TestDB2";
    int partitions = 100;
    String DB2tag = "TestDB2_tag";
    int replica = 2;
    for(int i = 0; i < 2; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, instanceName, DB2tag);
    }
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB2, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB2, DB2tag, replica);
    
    boolean result = ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
        CLUSTER_NAME)));
    Assert.assertTrue(result, "Cluster verification fails");
    
    ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(DB2));
    Set<String> hosts = new HashSet<String>();
    for(String p : ev.getPartitionSet())
    {
      for(String hostName : ev.getStateMap(p).keySet())
      {
        InstanceConfig config = accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag(DB2tag));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 2);
    
    String DB3 = "TestDB3";
    String DB3Tag = "TestDB3_tag";
    partitions = 10;
    replica = 3;
    for(int i = 1; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);
      _setupTool.getClusterManagementTool().addInstanceTag(CLUSTER_NAME, instanceName, DB3Tag);
    }
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB3, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB3, DB3Tag, replica);
    
    result = ClusterStateVerifier.verifyByZkCallback((new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
        CLUSTER_NAME)));
    Assert.assertTrue(result, "Cluster verification fails");
    
    ev = accessor.getProperty(accessor.keyBuilder().externalView(DB3));
    hosts = new HashSet<String>();
    for(String p : ev.getPartitionSet())
    {
      for(String hostName : ev.getStateMap(p).keySet())
      {
        InstanceConfig config = accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag(DB3Tag));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 4);
  }
}
