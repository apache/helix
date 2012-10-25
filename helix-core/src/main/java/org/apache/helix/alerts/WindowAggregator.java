package org.apache.helix.alerts;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Iterator;

import org.apache.helix.HelixException;


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
		
		Iterator<String> currTimeIter = currTimeTup.iterator();
		Iterator<String> currValIter = currValTup.iterator();
		Iterator<String> newTimeIter = newTimeTup.iterator();
		Iterator<String> newValIter = newValTup.iterator();
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
