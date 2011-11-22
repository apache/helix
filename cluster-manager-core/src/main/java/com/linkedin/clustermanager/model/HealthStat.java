package com.linkedin.clustermanager.model;

import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public class HealthStat {
	private final ZNRecord _record;

	public HealthStat(ZNRecord record)
	{
		_record = new ZNRecord(record);
	}
	
	public String getId()
	{
		return _record.getId();
	}
	
	public String getTestField()
	{
		return _record.getSimpleField("requestCountStat");
	}
	
	public Map<String, Map<String, String>> getMapFields()
	{
		return _record.getMapFields();
	}
}
