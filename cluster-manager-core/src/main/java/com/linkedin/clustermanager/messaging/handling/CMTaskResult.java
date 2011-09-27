package com.linkedin.clustermanager.messaging.handling;

import java.util.HashMap;
import java.util.Map;

public class CMTaskResult
{

  private boolean _success;
  private String _message;
  private Map<String, String> _taskResultMap = new HashMap<String, String>();

  public boolean isSucess()
  {
    return _success;
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
}
