/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.alerts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.stages.HealthDataCache;
import org.apache.helix.model.AlertStatus;
import org.apache.helix.model.Alerts;
import org.apache.log4j.Logger;


public class AlertsHolder {

	private static final Logger logger = Logger
		    .getLogger(AlertsHolder.class.getName());

	HelixDataAccessor _accessor;
	HealthDataCache _cache;
	Map<String, Map<String,String>> _alertsMap; //not sure if map or set yet
	Map<String, Map<String,String>> _alertStatusMap;
	//Alerts _alerts;
	HashSet<String> alerts;
	StatsHolder _statsHolder;

  private final HelixManager _manager;

  private Builder _keyBuilder;

	public AlertsHolder(HelixManager manager, HealthDataCache cache)
	{
	  this(manager, cache, new StatsHolder(manager, cache));
	}
	
	public AlertsHolder(HelixManager manager, HealthDataCache cache, StatsHolder statHolder)
  {
    _manager = manager;
    _accessor = manager.getHelixDataAccessor();
    _cache = cache;
    _statsHolder = statHolder;
    _keyBuilder = new PropertyKey.Builder(_manager.getClusterName());
    updateCache(_cache);
  }

	public void refreshAlerts()
	{
	  _cache.refresh(_accessor);
		updateCache(_cache);


		/*
		_alertsMap = _cache.getAlerts();
		//TODO: confirm this a good place to init the _statMap when null
		if (_alertsMap == null) {
			_alertsMap = new HashMap<String, Map<String,String>>();
		}\*/
	}

	public void refreshAlertStatus()
	{
		AlertStatus alertStatusRecord = _cache.getAlertStatus();
		if (alertStatusRecord != null) {
		_alertStatusMap = alertStatusRecord.getMapFields();
		}
		else {
			_alertStatusMap = new HashMap<String, Map<String,String>>();
		}
	}

	public void persistAlerts()
	{
		//XXX: Am I using _accessor too directly here?
	  
		Alerts alerts = _accessor.getProperty(_keyBuilder.alerts());
    if (alerts == null) {
      alerts = new Alerts(Alerts.nodeName); //TODO: fix naming of this record, if it matters
		}
		alerts.getRecord().setMapFields(_alertsMap);
		 boolean retVal = _accessor.setProperty(_keyBuilder.alerts(), alerts);
		 logger.debug("persistAlerts retVal: "+retVal);
	}

	public void persistAlertStatus()
	{
		//XXX: Am I using _accessor too directly here?
	  AlertStatus alertStatus = _accessor.getProperty(_keyBuilder.alertStatus());
		if (alertStatus == null) {
		  alertStatus = new AlertStatus(AlertStatus.nodeName); //TODO: fix naming of this record, if it matters
		}
		alertStatus.getRecord().setMapFields(_alertStatusMap);
		boolean retVal = _accessor.setProperty(_keyBuilder.alertStatus(), alertStatus);
		logger.debug("persistAlerts retVal: "+retVal);
	}

	//read alerts from cm state
	private void readExistingAlerts()
	{

	}

	public void addAlert(String alert) throws HelixException
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
		try
		{
		  alertFields.put(AlertParser.ACTION_NAME,
	        AlertParser.getComponent(AlertParser.ACTION_NAME, alert));
		}
		catch(Exception e)
		{
		  logger.info("No action specified in " + alert);
		}
		//store the expression as stat
		_statsHolder.addStat(alertFields.get(AlertParser.EXPRESSION_NAME));
        _statsHolder.persistStats();

		//naming the alert with the full name
		_alertsMap.put(alert, alertFields);
		persistAlerts();
	}

	/*
	 * Add a set of alert statuses to ZK
	 */
	public void addAlertStatusSet(Map<String, Map<String, AlertValueAndStatus>> statusSet) throws HelixException
	{
	  if (_alertStatusMap == null) {
	    _alertStatusMap = new HashMap<String, Map<String,String>>();
	  }
	  _alertStatusMap.clear(); //clear map.  all alerts overwrite old alerts
	  for (String alert : statusSet.keySet()) {
	    Map<String,AlertValueAndStatus> currStatus = statusSet.get(alert);
	    if (currStatus != null) {
	      addAlertStatus(alert, currStatus);
	    }
	  }

	  AlertStatus alertStatus = _accessor.getProperty(_keyBuilder.alertStatus());
	  int alertStatusSize = 0;
	  if (alertStatus != null) {
	    alertStatusSize = alertStatus.getMapFields().size();
	  }
	  //no need to persist alerts if there are none to persist and none are currently persisted
	 if (_alertStatusMap.size() > 0  || alertStatusSize > 0) {
	  persistAlertStatus(); //save statuses in zk
	 }
	}

	private void addAlertStatus(String parentAlertKey, Map<String,AlertValueAndStatus> alertStatus) throws HelixException
	{
		//_alertStatusMap = new HashMap<String,Map<String,String>>();
		for (String alertName : alertStatus.keySet()) {
			String mapAlertKey;
			mapAlertKey = parentAlertKey;
			if (!alertName.equals(ExpressionParser.wildcardChar)) {
				mapAlertKey = mapAlertKey+" : ("+alertName+")";
			}
			AlertValueAndStatus vs = alertStatus.get(alertName);
			Map<String,String> alertFields = new HashMap<String,String>();
			alertFields.put(AlertValueAndStatus.VALUE_NAME, vs.getValue().toString());
			alertFields.put(AlertValueAndStatus.FIRED_NAME, String.valueOf(vs.isFired()));
			_alertStatusMap.put(mapAlertKey, alertFields);
		}
	}

	public AlertValueAndStatus getAlertValueAndStatus(String alertName)
	{
		Map<String,String> alertFields = _alertStatusMap.get(alertName);
		String val = alertFields.get(AlertValueAndStatus.VALUE_NAME);
		Tuple<String> valTup = new Tuple<String>();
		valTup.add(val);
		boolean fired = Boolean.valueOf(alertFields.get(AlertValueAndStatus.FIRED_NAME));
		AlertValueAndStatus vs = new AlertValueAndStatus(valTup, fired);
		return vs;
	}

	public static void parseAlert(String alert, StringBuilder statsName,
			Map<String,String> alertFields) throws HelixException
	{
		alert = alert.replaceAll("\\s+", ""); //remove white space
		AlertParser.validateAlert(alert);
		//alertFields = new HashMap<String,String>();
		alertFields.put(AlertParser.EXPRESSION_NAME,
				AlertParser.getComponent(AlertParser.EXPRESSION_NAME, alert));
		alertFields.put(AlertParser.COMPARATOR_NAME,
				AlertParser.getComponent(AlertParser.COMPARATOR_NAME, alert));
		alertFields.put(AlertParser.CONSTANT_NAME,
				AlertParser.getComponent(AlertParser.CONSTANT_NAME, alert));
		try
        {
          alertFields.put(AlertParser.ACTION_NAME,
          AlertParser.getComponent(AlertParser.ACTION_NAME, alert));
        }
        catch(Exception e)
        {
           logger.info("No action specified in " + alert);
        }
		statsName.append(alertFields.get(AlertParser.EXPRESSION_NAME));
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
			Tuple<String> con = Tuple.fromString(alertFields.get(AlertParser.CONSTANT_NAME));
			//TODO: test the fields for null and fail if needed

			Alert a = new Alert(alert, exp, comp, con);
			alerts.add(a);
		}
		return alerts;
	}

  public void updateCache(HealthDataCache cache)
  {
    _cache = cache;
    Alerts alertsRecord = _cache.getAlerts();
    if (alertsRecord != null) 
    {
      _alertsMap = alertsRecord.getMapFields();
    }
    else 
    {
      _alertsMap = new HashMap<String, Map<String,String>>();
    }
  }
  
  public Map<String, Map<String,String>> getAlertsMap()
  {
    return _alertsMap;
  }
}
