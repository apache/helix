package com.linkedin.clustermanager.alerts;

import java.util.Iterator;
import java.util.List;

import com.linkedin.clustermanager.healthcheck.StatHealthReportProvider;

public class AlertProcessor {
	
	StatsHolder _statsHolder;
	//AlertsHolder _alertsHolder;
	
	/*
	public AlertProcessor(StatHealthReportProvider statProvider)
	{
		
	}
	*/
	
	public AlertProcessor(StatsHolder sh)
	{
		_statsHolder = sh;
	}
	
	public static void executeAlert(String alert, StatsHolder sh)
	{
		
	}
	
	public static void executeAllAlerts(List<Alert> alerts, List<Stat> stats)
	{
		for (Alert alert : alerts) {
			
		}
	}
}
