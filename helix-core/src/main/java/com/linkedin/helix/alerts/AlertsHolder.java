package com.linkedin.helix.alerts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.stages.HealthDataCache;
import com.linkedin.helix.model.AlertStatus;
import com.linkedin.helix.model.Alerts;

public class AlertsHolder {

	private static final Logger logger = Logger
		    .getLogger(AlertsHolder.class.getName());

	DataAccessor _accessor;
	HealthDataCache _cache;
	Map<String, Map<String,String>> _alertsMap; //not sure if map or set yet
	Map<String, Map<String,String>> _alertStatusMap;
	//Alerts _alerts;
	HashSet<String> alerts;
	StatsHolder _statsHolder;

	public AlertsHolder(HelixManager manager, HealthDataCache cache)
	{
		_accessor = manager.getDataAccessor();
		_cache = cache; // new ClusterDataCache();
		_statsHolder = new StatsHolder(manager, cache);
	}

	public void refreshAlerts()
	{
		_cache.refresh(_accessor);

		Alerts alertsRecord = _cache.getAlerts();
		if (alertsRecord != null) {
		_alertsMap = alertsRecord.getMapFields();
		}
		else {
			_alertsMap = new HashMap<String, Map<String,String>>();
		}


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
		ZNRecord alertsRec = _accessor.getProperty(PropertyType.ALERTS);
		if (alertsRec == null) {
			alertsRec = new ZNRecord(Alerts.nodeName); //TODO: fix naming of this record, if it matters
		}
		alertsRec.setMapFields(_alertsMap);
		 boolean retVal = _accessor.setProperty(PropertyType.ALERTS, alertsRec);
		 logger.debug("persistAlerts retVal: "+retVal);
	}

	public void persistAlertStatus()
	{
		//XXX: Am I using _accessor too directly here?
		ZNRecord alertStatusRec = _accessor.getProperty(PropertyType.ALERT_STATUS);
		if (alertStatusRec == null) {
			alertStatusRec = new ZNRecord(AlertStatus.nodeName); //TODO: fix naming of this record, if it matters
		}
		alertStatusRec.setMapFields(_alertStatusMap);
		boolean retVal = _accessor.setProperty(PropertyType.ALERT_STATUS, alertStatusRec);
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
		//store the expression as stat
		_statsHolder.addStat(alertFields.get(AlertParser.EXPRESSION_NAME));

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

	  AlertStatus alertStatus = _accessor.getProperty(AlertStatus.class, PropertyType.ALERT_STATUS);
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
		refreshAlerts();
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
}
