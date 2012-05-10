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
package com.linkedin.helix.model;

import java.util.Date;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.model.Message.Attributes;

/**
 * Instance that connects to zookeeper
 */
public class LiveInstance extends ZNRecordDecorator
{
  public enum LiveInstanceProperty
  {
    SESSION_ID,
    HELIX_VERSION,
    LIVE_INSTANCE
  }

  private static final Logger _logger = Logger.getLogger(LiveInstance.class.getName());

  public LiveInstance(String id)
  {
    super(id);
  }

  public LiveInstance(ZNRecord record)
  {
    super(record);
  }

  public void setSessionId(String sessionId)
  {
    _record.setSimpleField(LiveInstanceProperty.SESSION_ID.toString(), sessionId);
  }

  public String getSessionId()
  {
    return _record.getSimpleField(LiveInstanceProperty.SESSION_ID.toString());
  }

  public String getInstanceName()
  {
    return _record.getId();
  }

  public String getHelixVersion()
  {
    return _record.getSimpleField(LiveInstanceProperty.HELIX_VERSION.toString());
  }

  public void setHelixVersion(String helixVersion)
  {
    _record.setSimpleField(LiveInstanceProperty.HELIX_VERSION.toString(), helixVersion);
  }

  public String getLiveInstance()
  {
    return _record.getSimpleField(LiveInstanceProperty.LIVE_INSTANCE.toString());
  }

  public void setLiveInstance(String leader)
  {
    _record.setSimpleField(LiveInstanceProperty.LIVE_INSTANCE.toString(), leader);
  }

  public long getModifiedTime()
  {
	  return _record.getModifiedTime();
  }

  @Override
  public boolean isValid()
  {
    if(getSessionId() == null)
    {
      _logger.error("liveInstance does not have session id. id:" + _record.getId());
      return false;
    }
    if(getHelixVersion() == null)
    {
      _logger.error("liveInstance does not have CLM verion. id:" + _record.getId());
      return false;
    }
    return true;
  }
}
