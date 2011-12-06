package com.linkedin.clustermanager.alerts;

import java.util.Iterator;

import com.linkedin.clustermanager.ClusterManagerException;

public class WindowAggregator extends Aggregator {

	
	int _windowSize;
	
	public WindowAggregator(String windowSize) 
	{
		_windowSize = Integer.parseInt(windowSize);
		_numArgs = 1;
	}
	
	public WindowAggregator()
	{
		this("1");
	}

	@Override
	public void merge(Tuple<String> currValTup, Tuple<String> newValTup,
			Tuple<String> currTimeTup, Tuple<String> newTimeTup, String... args) {
		
		_windowSize = Integer.parseInt(args[0]);
		
		//figure out how many curr tuple values we displace
		Tuple<String> mergedTimeTuple = new Tuple<String>();
		Tuple<String> mergedValTuple = new Tuple<String>();
		
		Iterator<String> currTimeIter = currTimeTup.getIterator();
		Iterator<String> currValIter = currValTup.getIterator();
		Iterator<String> newTimeIter = newTimeTup.getIterator();
		Iterator<String> newValIter = newValTup.getIterator();
		int currCtr = 0;
		//traverse current vals
		double currTime = -1;
		double currVal;
		while (currTimeIter.hasNext()) {
			currTime = Double.parseDouble(currTimeIter.next());
			currVal = Double.parseDouble(currValIter.next());
			currCtr++;
			//number of evicted currVals equal to total size of both minus _windowSize
			if (currCtr > (newTimeTup.size()+currTimeTup.size()-_windowSize)) { //non-evicted element, just bump down
				mergedTimeTuple.add(String.valueOf(currTime));
				mergedValTuple.add(String.valueOf(currVal));
			}
		}

		double newVal;
		double newTime;
		while (newTimeIter.hasNext()) {
			newVal = Double.parseDouble(newValIter.next());
			newTime = Double.parseDouble(newTimeIter.next());
			if (newTime <= currTime) { //oldest new time older than newest curr time.  we will not apply new tuple!
				return; //curr tuples remain the same
			}
			currCtr++;
			if (currCtr > (newTimeTup.size()+currTimeTup.size()-_windowSize)) { //non-evicted element
				mergedTimeTuple.add(String.valueOf(newTime));
				mergedValTuple.add(String.valueOf(newVal));
			}
		}
		 //set curr tuples to merged tuples
		currTimeTup.clear();
		currTimeTup.addAll(mergedTimeTuple);
		currValTup.clear();
		currValTup.addAll(mergedValTuple);
		//TODO: see if we can do merger in place on curr
	}

	
}
