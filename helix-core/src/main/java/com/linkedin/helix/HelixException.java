package com.linkedin.helix;


public class HelixException extends RuntimeException
{

  public HelixException(String message)
  {
    super(message);
  }

  public HelixException(Throwable cause)
  {
    super(cause);
  }
}
