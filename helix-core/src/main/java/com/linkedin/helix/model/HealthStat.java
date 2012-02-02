package com.linkedin.helix.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
import com.linkedin.helix.alerts.ExpressionParser;
import com.linkedin.helix.alerts.StatsHolder;
import com.linkedin.helix.model.Message.Attributes;

public class HealthStat extends ZNRecordDecorator 
{
	public enum HealthStatProperty
	{
		FIELDS
	}
	private static final Logger _logger = Logger.getLogger(HealthStat.class.getName());

	public HealthStat(String id)
	{
		super(id);
	}

	public HealthStat(ZNRecord record)
	  {
	    super(record);
	    if(getCreateTimeStamp() == 0)
	    {
	      _record.setSimpleField(Attributes.CREATE_TIMESTAMP.toString(), ""
	          + new Date().getTime());
	    }
	  }
	
  public long getCreateTimeStamp()
  {
    if (_record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()) == null)
    {
      return 0;
    }
    try
    {
      return Long.parseLong(_record.getSimpleField(Attributes.CREATE_TIMESTAMP.toString()));
    } catch (Exception e)
    {
      return 0;
    }
  }
	
	public String getTestField()
	{
		return _record.getSimpleField("requestCountStat");
	}
	
	public void setHealthFields(Map<String, Map<String, String>> healthFields)
	{
		_record.setMapFields(healthFields);
	}
	
	public String buildCompositeKey(String instance, String parentKey, String statName ) {
		String delim = ExpressionParser.statFieldDelim;
		return instance+delim+parentKey+delim+statName;
	}
	
	public Map<String, Map<String, String>> getHealthFields(String instanceName, String timestamp)
	{
		//XXX: need to do some conversion of input format to the format that stats computation wants
		Map<String, Map<String, String>> currMapFields = _record.getMapFields();
		Map<String, Map<String, String>> convertedMapFields = new HashMap<String, Map<String, String>>();
		for (String key : currMapFields.keySet()) {
			Map<String, String> currMap = currMapFields.get(key);
			for (String subKey : currMap.keySet()) {
				String compositeKey = buildCompositeKey(instanceName, key, subKey);
				String value = currMap.get(subKey);
				Map<String, String> convertedMap = new HashMap<String, String>();
				convertedMap.put(StatsHolder.VALUE_NAME, value);
				convertedMap.put(StatsHolder.TIMESTAMP_NAME, timestamp);
				convertedMapFields.put(compositeKey, convertedMap);
			}
			
			
		}
		
		//return _record.getMapFields();
		return convertedMapFields;
	}

	
	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}
}
