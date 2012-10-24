package org.apache.helix.integration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestBasicSpectator extends ZkStandAloneCMTestBase implements ExternalViewChangeListener
{
  Map<String, Integer> _externalViewChanges = new HashMap<String, Integer>();
  
  @Test
  public void TestSpectator() throws Exception
  {
    HelixManager relayHelixManager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME,
        null,
        InstanceType.SPECTATOR,
        ZK_ADDR);

    relayHelixManager.connect();
    relayHelixManager.addExternalViewChangeListener(this);
    
    _setupTool.addResourceToCluster(CLUSTER_NAME, "NextDB", 64, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "NextDB", 3);
    
    boolean result = ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
    
    Assert.assertTrue(_externalViewChanges.containsKey("NextDB"));
    Assert.assertTrue(_externalViewChanges.containsKey(TEST_DB));
    
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext)
  {
    for(ExternalView view : externalViewList)
    {
      if(!_externalViewChanges.containsKey(view.getResourceName()))
      {
        _externalViewChanges.put(view.getResourceName(), 1);
      }
      else
      {
        _externalViewChanges.put(view.getResourceName(), _externalViewChanges.get(view.getResourceName())+ 1);
      }
    }
  }
}
