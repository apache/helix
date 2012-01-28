package com.linkedin.helix.controller.pipeline;

public class StageException extends Exception
{

  public StageException(String message)
  {
    super(message);
  }
  public StageException(String message,Exception e)
  {
    super(message,e);
  }
}
