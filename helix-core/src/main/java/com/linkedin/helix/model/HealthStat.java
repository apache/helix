package com.linkedin.helix.model;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZNRecordDecorator;
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
	
	public Map<String, Map<String, String>> getHealthFields()
	{
		return _record.getMapFields();
	}

	@Override
	public boolean isValid() {
		// TODO Auto-generated method stub
		return false;
	}
}
