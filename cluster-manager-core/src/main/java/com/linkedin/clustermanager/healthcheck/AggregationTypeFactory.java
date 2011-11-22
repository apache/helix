package com.linkedin.clustermanager.healthcheck;

import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class AggregationTypeFactory {
	private static final Logger logger = Logger.getLogger(AggregationTypeFactory.class);
	
	public AggregationTypeFactory()
	{
	}
	
	//TODO: modify this function so that it takes a single string, but can parse apart params from type
	public static AggregationType getAggregationType(String input) {
		if (input == null) {
			logger.error("AggregationType name is null");
			return null;
		}
		StringTokenizer tok = new StringTokenizer(input, AggregationType.DELIM);
		String type = tok.nextToken();
		int numParams = tok.countTokens();
		String[] params = new String[numParams];
		for (int i=0;i<numParams;i++) {
			if (!tok.hasMoreTokens()) {
				logger.error("Trying to parse non-existent params");
				return null;
			}
			params[i] = tok.nextToken();
		}
		
		
		if (type.equals(AccumulateAggregationType.TYPE_NAME)) {
			return new AccumulateAggregationType();
		}
		else if (type.equals(DecayAggregationType.TYPE_NAME)) {
			if (params.length < 1) {
				logger.error("DecayAggregationType must contain <decay weight> parameter");
				return null;
			}
			return new DecayAggregationType(Double.parseDouble(params[0]));
		}
		else if (type.equals(WindowAggregationType.TYPE_NAME)) {
			if (params.length < 1) {
				logger.error("WindowAggregationType must contain <window size> parameter");
			}
			return new WindowAggregationType(Integer.parseInt(params[0]));
		}
		else {
			logger.error("Unknown AggregationType "+type);
			return null;
		}
	}
}
