package com.linkedin.clustermanager.model;

import java.util.Map;

import org.apache.zookeeper.data.Stat;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZNRecordAndStat;

public class HealthStat extends ZNRecordAndStat {
	//private final ZNRecord _record;

	public HealthStat(ZNRecord record)
	{
		this(record, null);
	}
	
	public HealthStat(ZNRecord record, Stat stat)
	{
		super(record, stat);
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
