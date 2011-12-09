package com.linkedin.clustermanager.alerts;

import java.util.Iterator;

import com.linkedin.clustermanager.ClusterManagerException;

public class AccumulateAggregator extends Aggregator {

	
	public AccumulateAggregator() 
	{
		_numArgs = 0;
	}
	
	@Override
	public void merge(Tuple<String> currValTup, Tuple<String> newValTup,
			Tuple<String> currTimeTup, Tuple<String> newTimeTup, String... args) {
	
		double currVal = 0;
		double currTime = -1;
		double newVal;
		double newTime;
		double mergedVal;
		double mergedTime;
		
		if (currValTup == null || newValTup == null || currTimeTup == null ||
				newTimeTup == null) {
			throw new ClusterManagerException("Tuples cannot be null");
		}
		
		//old tuples may be empty, indicating no value/time exist
		if (currValTup.size() > 0 && currTimeTup.size() > 0) {
			currVal = Double.parseDouble(currValTup.iterator().next());
			currTime = Double.parseDouble(currTimeTup.iterator().next());
		}
		newVal = Double.parseDouble(newValTup.iterator().next());
		newTime = Double.parseDouble(newTimeTup.iterator().next());
		
		if (newTime > currTime) { //if old doesn't exist, we end up here
			mergedVal = currVal+newVal; //if old doesn't exist, it has value "0"
			mergedTime = newTime;
		}
		else {
			mergedVal = currVal;
			mergedTime = currTime;
		}
	
		currValTup.clear();
		currValTup.add(Double.toString(mergedVal));
		currTimeTup.clear();
		currTimeTup.add(Double.toString(mergedTime));
	}

	
}
