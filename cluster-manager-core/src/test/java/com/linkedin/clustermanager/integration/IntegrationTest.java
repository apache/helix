package com.linkedin.clustermanager.integration;

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

public class IntegrationTest extends ZkStandAloneCMTestBase
{
  @Test
  public void integrationTest() throws Exception
  {
    AssertJUnit.assertTrue(ClusterStateVerifier.verifyClusterStates(ZK_ADDR, CLUSTER_NAME));

//    ClusterManager manager = _startCMResultMap.get("controller_0")._manager;
//    boolean result = verifyBestPossibleAndExternalView(TEST_DB, 20, manager);
//    System.out.println("verifyBestPossibleAndExternalView() result:" + result);
//    System.out.println("STOP integrationTest() at " + new Date(System.currentTimeMillis()));
  }



}
