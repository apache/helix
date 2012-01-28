package com.linkedin.helix.messaging.handling;

import java.util.HashMap;
import java.util.Map;

public class CMTaskResult
{

  private boolean _success;
  private String _message = "";
  private Map<String, String> _taskResultMap = new HashMap<String, String>();
  private boolean _interrupted = false;
  Exception _exception = null;
  
  public boolean isSucess()
  {
    return _success;
  }
  
  public boolean isInterrupted()
  {
    return _interrupted; 
  }
  
  public void setInterrupted(boolean interrupted)
  {
    _interrupted = interrupted;
  }
  
  public void setSuccess(boolean success)
  {
    this._success = success;
  }

  public String getMessage()
  {
    return _message;
  }

  public void setMessage(String message)
  {
    this._message = message;
  }
  
  public Map<String, String> getTaskResultMap()
  {
    return _taskResultMap;
  }
  
  public void setException(Exception e)
  {
    _exception = e;
  }
  
  public Exception getException()
  {
    return _exception;
  }
}
