package com.linkedin.helix.model;


import java.util.Map;


import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;

public class AlertStatus extends ZNRecordDecorator
{


	
	
	public final static String nodeName = "AlertStatus";
	
	
	public enum AlertsProperty
	  {
	    SESSION_ID,
	    FIELDS
	  }
	
	
	public AlertStatus(String id) 
	{
		super(id);
	}
	
  public AlertStatus(ZNRecord record)
  {
//    _record = record;
    super(record);

  }

  /*
  public Alerts(ZNRecord record, Stat stat)
  {
    super(record, stat);
  }
*/
  
  public void setSessionId(String sessionId){
    _record.setSimpleField(AlertsProperty.SESSION_ID.toString(), sessionId);
  }
  public String getSessionId()
  {
    return _record.getSimpleField(AlertsProperty.SESSION_ID.toString());
  }

  public String getInstanceName()
  {
    return _record.getId();
  }

  /*
  public String getVersion()
  {
    return _record.getSimpleField(AlertsProperty.CLUSTER_MANAGER_VERSION.toString());
  }
  */
  
  public Map<String, Map<String, String>> getMapFields() {
	  return _record.getMapFields();
  }
  
  
  public Map<String, String> getStatFields(String statName) {
	  return _record.getMapField(statName);
  }

@Override
public boolean isValid() {
	// TODO Auto-generated method stub
	return false;
}
}
