package com.linkedin.clustermanager.healthcheck;

import org.apache.log4j.Logger;

public class AccumulateAggregationType implements AggregationType {

	private static final Logger logger = Logger.getLogger(AccumulateAggregationType.class);
	
	public final static String TYPE_NAME="accumulate";
	
	@Override
	public String getName() {
		return TYPE_NAME;
	}

	@Override
	public String merge(String iv, String ev, long prevTimestamp) {
		double inVal = Double.parseDouble(iv);
		double existingVal = Double.parseDouble(ev);
		return String.valueOf(inVal + existingVal);
	}
}
