package com.linkedin.clustermanager.model;

import static com.linkedin.clustermanager.CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION;
import static com.linkedin.clustermanager.CMConstants.ZNAttribute.SESSION_ID;

import java.util.Map;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;

public class Alerts extends ZNRecordAndStat
{

//  private final ZNRecord _record;

	public final static String nodeName = "Alerts";
	
  public Alerts(ZNRecord record)
  {
//    _record = record;
    super(record);

  }

  public Alerts(ZNRecord record, Stat stat)
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
  
  public Map<String, Map<String, String>> getMapFields() {
	  return _record.getMapFields();
  }
  
  
  public Map<String, String> getStatFields(String statName) {
	  return _record.getMapField(statName);
  }
}
