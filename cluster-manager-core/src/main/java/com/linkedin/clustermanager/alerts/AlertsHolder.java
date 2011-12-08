package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.controller.stages.ClusterDataCache;

public class AlertsHolder {
	
	private static final Logger logger = Logger
		    .getLogger(AlertsHolder.class.getName());
	
	ClusterDataAccessor _accessor;
	ClusterDataCache _cache;
	Map<String, Map<String,String>> _alertsMap; //not sure if map or set yet
	HashSet<String> alerts;
	
	public AlertsHolder(ClusterManager manager) 
	{
		_accessor = manager.getDataAccessor();
		_cache = new ClusterDataCache();
	}
	
	public void refreshAlerts()
	{
		_cache.refresh(_accessor);
		_alertsMap = _cache.getAlerts();
		//TODO: confirm this a good place to init the _statMap when null
		if (_alertsMap == null) {
			_alertsMap = new HashMap<String, Map<String,String>>();
		}
	}
	
	public void persistAlerts() 
	{
		//XXX: Am I using _accessor too directly here?
		ZNRecord alertsRec = _accessor.getProperty(PropertyType.ALERTS);
		if (alertsRec == null) {
			alertsRec = new ZNRecord("PersistentStats"); //TODO: fix naming of this record, if it matters
		}
		alertsRec.setMapFields(_alertsMap);
		 boolean retVal = _accessor.setProperty(PropertyType.ALERTS, alertsRec);
		 logger.debug("persistAlerts retVal: "+retVal);
	}
	
	//read alerts from cm state
	private void readExistingAlerts() 
	{
		
	}
	
	public void addAlert(String alert) throws ClusterManagerException
	{
		alert = alert.replaceAll("\\s+", ""); //remove white space
		AlertParser.validateAlert(alert);
		refreshAlerts();
		//stick the 3 alert fields in map
		Map<String, String> alertFields = new HashMap<String,String>();
		alertFields.put(AlertParser.EXPRESSION_NAME, 
				AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert));
		alertFields.put(AlertParser.COMPARATOR_NAME, 
				AlertParser.getComponent(AlertParser.COMPARATOR_NAME, alert));
		alertFields.put(AlertParser.CONSTANT_NAME, 
				AlertParser.getComponent(AlertParser.CONSTANT_NAME, alert));
		//naming the alert with the full name
		_alertsMap.put(alert, alertFields); 
		persistAlerts();
	}
	
	/*
	public void evaluateAllAlerts() 
	{
		for (String alert : _alertsMap.keySet()) {
			Map<String,String> alertFields = _alertsMap.get(alert);
			String exp = alertFields.get(AlertParser.EXPRESSION_NAME);
			String comp = alertFields.get(AlertParser.COMPARATOR_NAME);
			String con = alertFields.get(AlertParser.CONSTANT_NAME);
			//TODO: test the fields for null and fail if needed
			
			AlertProcessor.execute(exp,  comp, con, sh);
		}
	}
	*/
	
	public List<Alert> getAlertList()
	{
		List<Alert> alerts = new LinkedList<Alert>();
		for (String alert : _alertsMap.keySet()) {
			Map<String,String> alertFields = _alertsMap.get(alert);
			String exp = alertFields.get(AlertParser.EXPRESSION_NAME);
			String comp = alertFields.get(AlertParser.COMPARATOR_NAME);
			String con = alertFields.get(AlertParser.CONSTANT_NAME);
			//TODO: test the fields for null and fail if needed
			
			Alert a = new Alert(alert, exp, comp, con);
			alerts.add(a);
		}
		return alerts;
	}
}
