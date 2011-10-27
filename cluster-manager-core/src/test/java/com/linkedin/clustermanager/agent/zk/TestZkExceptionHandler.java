package com.linkedin.clustermanager.agent.zk;

import java.util.Date;

import org.I0Itec.zkclient.exception.ZkInterruptedException;

public class TestZkExceptionHandler
{
  // @Test(groups = { "unitTest" })
  public void testZkExceptionHandler()
  {
  	System.out.println("START TestZkExceptionHandler at " + new Date(System.currentTimeMillis()));
    final String msg = "testZkExceptionHandler: IGNORE THIS EXCEPTION.THIS IS PART OF UNIT TEST";
    InterruptedException e = new InterruptedException(msg);
    ZKExceptionHandler.getInstance().handle(new ZkInterruptedException(e));
    // ZKExceptionHandler.getInstance().handle(new Exception(e));
    System.out.println("END TestZkExceptionHandler at " + new Date(System.currentTimeMillis()));
  }
}
