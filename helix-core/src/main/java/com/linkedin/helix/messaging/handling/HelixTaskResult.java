/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.messaging.handling;

import java.util.HashMap;
import java.util.Map;

public class HelixTaskResult
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
