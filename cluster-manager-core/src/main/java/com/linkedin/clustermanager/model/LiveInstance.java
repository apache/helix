package com.linkedin.clustermanager.model;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordDecorator;

/**
 * Instance that connects to zookeeper
 */
public class LiveInstance extends ZNRecordDecorator
{
  public enum LiveInstanceProperty
  {
    SESSION_ID,
    CLUSTER_MANAGER_VERSION,
    LEADER
  }

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

  public String getClusterManagerVersion()
  {
    return _record.getSimpleField(LiveInstanceProperty.CLUSTER_MANAGER_VERSION.toString());
  }

  public void setClusterManagerVersion(String clusterManagerVersion)
  {
    _record.setSimpleField(LiveInstanceProperty.CLUSTER_MANAGER_VERSION.toString(), clusterManagerVersion);
  }

  public String getLeader()
  {
    return _record.getSimpleField(LiveInstanceProperty.LEADER.toString());
  }

  public void setLeader(String leader)
  {
    _record.setSimpleField(LiveInstanceProperty.LEADER.toString(), leader);
  }

}
