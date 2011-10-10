package com.linkedin.clustermanager.agent.zk;

import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.log4j.Logger;

public class ZKExceptionHandler
{
  private static ZKExceptionHandler instance = new ZKExceptionHandler();
  private static Logger logger = Logger.getLogger(ZKExceptionHandler.class);
  private ZKExceptionHandler()
  {

  }

  void handle(Exception e)
  {
    if (e instanceof ZkInterruptedException)
    {
      logger.warn("zk connection is interrupted, exception" + e);
    }
    else
    {
      logger.error(e.getMessage(), e);
      // e.printStackTrace();
    }
  }

  public static ZKExceptionHandler getInstance()
  {
    return instance;
  }
}
