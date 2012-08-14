package com.linkedin.helix;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.testng.Assert;

public class IntegrationTestHelper
{
  private static final Logger LOG = Logger.getLogger(IntegrationTestHelper.class);

  public static final String INTEGRATION_SCRIPT_DIR = "src/main/scripts/integration-test/script";
  public static final String INTEGRATION_TEST_DIR = "src/main/scripts/integration-test/testcases";
  public static final long EXEC_TIMEOUT = 1200;
  
  public static String getPrefix()
  {
    StringBuilder prefixBuilder = new StringBuilder("");
    String prefix = "";
    String filepath = INTEGRATION_SCRIPT_DIR;
    File integrationScriptDir = new File(filepath);

    while (!integrationScriptDir.exists())
    {
      prefixBuilder.append("../");
      prefix = prefixBuilder.toString();

      integrationScriptDir = new File(prefix + filepath);

      // Give up
      if (prefix.length() > 30)
      {
        return "";
      }
    }
    return new File(prefix).getAbsolutePath() + "/";
  }
  
  public static ExternalCommand runCommandLineTest(String testName, String... arguments) throws IOException, InterruptedException,
  TimeoutException
  {
    ExternalCommand cmd = ExternalCommand.executeWithTimeout(new File(getPrefix() + INTEGRATION_TEST_DIR),
                                             testName, EXEC_TIMEOUT, arguments);
    int exitValue = cmd.exitValue();
    String output = cmd.getStringOutput("UTF8");

    if (0 == exitValue)
    {
      LOG.info("Test " + testName + " has run. ExitCode=" + exitValue + ". Command output: " + output);
    }
    else
    {
      LOG.warn("Test " + testName + " is FAILING. ExitCode=" + exitValue + ". Command output: " + output);
      Assert.fail(output);
//      return cmd;
    }
    return cmd;
  }
}

