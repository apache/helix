package com.linkedin.helix.model;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;

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
