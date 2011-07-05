package com.linkedin.clustermanager.test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;

import com.linkedin.dds.test.infra.DdsBaseIntegTest;

public class TestCMIntegrationOne extends DdsBaseIntegTest
{
  @BeforeTest
  public void setup() throws IOException, Exception
  {
    LOG.info("setup");
    // skip the super Setup. Just load the view root
    loadSystemProperties();
  }
  @Test
  public void testClmFileDummrprocess()
  throws SecurityException, NoSuchMethodException, IOException, InterruptedException, TimeoutException
  {
    LOG.info("Running clm_file_dummyprocess.test");
    runCommandLineTest("clm_file_dummyprocess.test");
  } 
}