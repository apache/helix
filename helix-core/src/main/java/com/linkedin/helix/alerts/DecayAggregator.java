package com.linkedin.helix.alerts;

import java.util.Iterator;

import com.linkedin.helix.ClusterManagerException;

public class DecayAggregator extends Aggregator {

	double _decayWeight;
	
	public DecayAggregator(double weight)
	{
		_decayWeight = weight;
	}
	
	public DecayAggregator() 
	{
		_numArgs = 1;
	}
	
	@Override
	public void merge(Tuple<String> currValTup, Tuple<String> newValTup,
			Tuple<String> currTimeTup, Tuple<String> newTimeTup, String... args) {
	
		_decayWeight = Double.parseDouble(args[0]);
		
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
			mergedVal = (1-_decayWeight)*currVal+_decayWeight*newVal; //if old doesn't exist, it has value "0"
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
