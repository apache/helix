package com.linkedin.helix.alerts;

public class AlertValueAndStatus {
	public final static String VALUE_NAME = "value";
	public final static String FIRED_NAME = "fired";
	
	private Tuple<String> value;
	private boolean fired;
	
	public AlertValueAndStatus(Tuple<String> value, boolean fired)
	{
		this.value = value;
		this.fired = fired;
	}

	public Tuple<String> getValue() {
		return value;
	}

	public boolean isFired() {
		return fired;
	}
	
}
