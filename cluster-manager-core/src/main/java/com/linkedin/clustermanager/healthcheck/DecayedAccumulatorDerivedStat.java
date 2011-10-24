package com.linkedin.clustermanager.healthcheck;

import org.apache.log4j.Logger;

public class DecayedAccumulatorDerivedStat extends DerivedStat {

	private static final Logger _logger = Logger.getLogger(DecayedAccumulatorDerivedStat.class);
	
	long value;
	long decayFactor;
	long lastUpdateTime;
	
	public DecayedAccumulatorDerivedStat(String opType, String measurementType,
			String resourceName, String partitionName, String nodeName,
			long decayFactor) {
		super(opType, measurementType, resourceName, partitionName, nodeName);
		this.decayFactor = decayFactor;
		if (decayFactor > 1) {
			_logger.warn("decayFactor is "+decayFactor+", greater than 1");
		}
		lastUpdateTime = -1;
	}

	@Override
	public void applyRawStat(RawStat rs) 
	{

		long newValue = rs.getValue();
		if (lastUpdateTime < 0) {
			value = newValue;
		}
		value = decayFactor*value + (1-decayFactor)*newValue;
		//TODO: figure out how to tweak weights depending on how long since lastUpdateTime
		
		lastUpdateTime = System.currentTimeMillis();
	}

}
