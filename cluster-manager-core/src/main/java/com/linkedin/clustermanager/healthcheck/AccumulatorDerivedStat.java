package com.linkedin.clustermanager.healthcheck;

public class AccumulatorDerivedStat extends DerivedStat {

	long value;
	
	public AccumulatorDerivedStat(String opType, String measurementType,
			String resourceName, String partitionName, String nodeName,
			long decayFactor) {
		super(opType, measurementType, resourceName, partitionName, nodeName);
	}

	@Override
	public void applyRawStat(RawStat rs) {
		value += rs.getValue();
	}

}
