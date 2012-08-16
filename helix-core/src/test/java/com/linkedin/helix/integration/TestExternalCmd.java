package com.linkedin.helix.integration;

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ExternalCommand;
import com.linkedin.helix.ScriptTestHelper;
import com.linkedin.helix.TestHelper;

public class TestExternalCmd
{

  @Test
  public void testExternalCmd() throws Exception
  {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));

    ExternalCommand cmd = ScriptTestHelper.runCommandLineTest("dummy.sh");
    String output = cmd.getStringOutput("UTF8");
    int idx = output.indexOf("this is a dummy test for verify ExternalCommand works");
    Assert.assertNotSame(idx, -1);
    
    System.out.println("END " + testName + " at "
        + new Date(System.currentTimeMillis()));

  }
}
