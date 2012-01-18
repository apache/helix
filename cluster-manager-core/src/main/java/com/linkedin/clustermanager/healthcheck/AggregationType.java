package com.linkedin.clustermanager.healthcheck;

public interface AggregationType {

	//public abstract <T extends Object> T merge(T iv, T ev);
	
	public final static String DELIM = "#";
	
	public abstract String merge(String incomingVal, String existingVal, long prevTimestamp);
	
	public abstract String getName();
}

