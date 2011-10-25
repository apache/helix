package com.linkedin.clustermanager.integration;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.AssertJUnit;
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
  @Test (groups = {"integrationTest"})
  public void integrationTest() throws Exception
  {
    AssertJUnit.assertTrue(ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME));
  }
}
