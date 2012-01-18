package com.linkedin.clustermanager.alerts;

public class AlertValueAndStatus {
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
