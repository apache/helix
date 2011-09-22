package com.linkedin.clustermanager;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.tools.ClusterStateVerifier;

/**
 * This is a simple integration test. We will use this until we have framework
 * which helps us write integration tests easily
 * 
 * @author kgopalak
 * 
 */
public class IntegrationTest extends ZkStandAloneCMHandler
{
  @Test
  public void integrationTest() throws Exception
  {
    Assert.assertTrue(ClusterStateVerifier.VerifyClusterStates(ZK_ADDR, CLUSTER_NAME));
  }
}
