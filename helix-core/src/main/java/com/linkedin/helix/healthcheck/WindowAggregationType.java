package com.linkedin.helix.healthcheck;

import java.util.TimerTask;

import org.apache.log4j.Logger;

public class WindowAggregationType implements AggregationType {

	private static final Logger logger = Logger.getLogger(WindowAggregationType.class);
	
	public final String WINDOW_DELIM = "#";
	
	public final static String TYPE_NAME="window";
	
	int _windowSize = 1;
	
	public WindowAggregationType(int ws)
	{
		super();
		_windowSize = ws;
	}
	
	@Override
	public String getName() {
		StringBuilder sb = new StringBuilder();
		sb.append(TYPE_NAME);
		sb.append(DELIM);
		sb.append(_windowSize);
		return sb.toString();
	}

	@Override
	public String merge(String incomingVal, String existingVal, long prevTimestamp) {
		String[] windowVals;
		if (existingVal == null) {
			return incomingVal;
		}
		else {
			windowVals = existingVal.split(WINDOW_DELIM);
			int currLength = windowVals.length;
			//window not full
			if (currLength < _windowSize) {
				return existingVal+WINDOW_DELIM+incomingVal;
			}
			//evict oldest
			else {
				int firstDelim = existingVal.indexOf(WINDOW_DELIM);
				return existingVal.substring(firstDelim+1) + WINDOW_DELIM +
						incomingVal;
			}
		}
	}
}
