package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ExternalCommand;
import com.linkedin.helix.IntegrationTestHelper;
import com.linkedin.helix.TestHelper;

public class TestFailOverPerf1kp
{
  @Test
  public void testFailOverPerf1kp() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));

    ExternalCommand cmd = IntegrationTestHelper.runCommandLineTest("helix_random_kill_local_startzk.sh");
    String output = cmd.getStringOutput("UTF8");
    int i = getStateTransitionLatency(0, output);
    int j = output.indexOf("ms", i);
    long latency = Long.parseLong(output.substring(i, j));
    System.out.println("startup latency: " + latency);
    
    i = getStateTransitionLatency(i, output);
    j = output.indexOf("ms", i);
    latency = Long.parseLong(output.substring(i, j));
    System.out.println("failover latency: " + latency);
    Assert.assertTrue(latency < 800, "failover latency for 1k partition test should < 800ms");
    
    System.out.println("END " + testName + " at "
        + new Date(System.currentTimeMillis()));

  }
  
  int getStateTransitionLatency(int start, String output)
  {
    final String pattern = "state transition latency: ";
    int i = output.indexOf(pattern, start) + pattern.length();
//    String latencyStr = output.substring(i, j);
//    System.out.println(latencyStr);
    return i;
  }
}
