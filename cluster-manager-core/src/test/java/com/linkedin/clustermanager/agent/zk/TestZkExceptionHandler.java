package com.linkedin.clustermanager.agent.zk;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.testng.annotations.Test;

public class TestZkExceptionHandler
{
  @Test(groups = { "unitTest" })
  public void testZkExceptionHandler()
  {
    final String msg = "testZkExceptionHandler";
    InterruptedException e = new InterruptedException(msg);
    ZKExceptionHandler.getInstance().handle(new ZkInterruptedException(e));
    ZKExceptionHandler.getInstance().handle(new Exception(e));
  }
}
