package com.linkedin.clustermanager.model;

import static com.linkedin.clustermanager.CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION;
import static com.linkedin.clustermanager.CMConstants.ZNAttribute.SESSION_ID;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;

public class LiveInstance extends ZNRecordAndStat
{

//  private final ZNRecord _record;

  public LiveInstance(ZNRecord record)
  {
//    _record = record;
    super(record);

  }

  public LiveInstance(ZNRecord record, Stat stat)
  {
    super(record, stat);
  }

  public void setSessionId(String sessionId){
    _record.setSimpleField(SESSION_ID.toString(), sessionId);
  }
  public String getSessionId()
  {
    return _record.getSimpleField(SESSION_ID.toString());
  }

  public String getInstanceName()
  {
    return _record.getId();
  }

  public String getVersion()
  {
    return _record.getSimpleField(CLUSTER_MANAGER_VERSION.toString());
  }
}
