package com.linkedin.clustermanager.integration;

import org.testng.annotations.Test;

import com.linkedin.clustermanager.TestHelper;

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
    TestHelper.verifyWithTimeout("verifyIdealAndCurState",
                                TestHelper.<String>setOf(CLUSTER_NAME),
                                ZK_ADDR);
  }
}
