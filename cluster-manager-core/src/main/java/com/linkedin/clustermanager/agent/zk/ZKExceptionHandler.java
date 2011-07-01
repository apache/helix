package com.linkedin.clustermanager.agent.zk;

public class ZKExceptionHandler
{
  private static ZKExceptionHandler instance = new ZKExceptionHandler();

  private ZKExceptionHandler()
  {

  }

  void handle(Exception e)
  {

    e.printStackTrace();
  }

  public static ZKExceptionHandler getInstance()
  {
    return instance;
  }
}
