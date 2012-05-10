/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.alerts;

import java.util.Iterator;
import java.util.List;

public abstract class Operator {
	
	public int minInputTupleLists;
	public int maxInputTupleLists;
	public int numOutputTupleLists = -1;
	public boolean inputOutputTupleListsCountsEqual = false;
	
	public Operator()
	{
		
	}
	
	public Tuple<String> multiplyTuples(Tuple<String> tup1, Tuple<String> tup2)
	{
		if (tup1 == null) {
			return tup2;
		}
		if (tup2 == null) {
			return tup1;
		}
		Tuple<String>outputTup = new Tuple<String>();
		

		//sum staggers if the tuples are same length
		//e.g. 1,2,3 + 4,5 = 1,6,8
		//so this is a bit tricky
		Tuple<String>largerTup;
		Tuple<String>smallerTup;
		if (tup1.size() >= tup2.size()) {
			largerTup = tup1;
			smallerTup = tup2;
		}
		else {
			largerTup = tup2;
			smallerTup = tup1;
		}		
		int gap = largerTup.size() - smallerTup.size();
		
		for (int i=0; i< largerTup.size();i++) {
			if (i < gap) {
				outputTup.add(largerTup.getElement(i));
			}
			else {
				double elementProduct = 0;
				elementProduct = Double.parseDouble(largerTup.getElement(i)) *
						Double.parseDouble(smallerTup.getElement(i-gap));
				outputTup.add(String.valueOf(elementProduct));
			}
		}
		return outputTup;
	}
	
	public Tuple<String> sumTuples(Tuple<String> tup1, Tuple<String> tup2)
	{
		if (tup1 == null) {
			return tup2;
		}
		if (tup2 == null) {
			return tup1;
		}
		Tuple<String>outputTup = new Tuple<String>();
		

		//sum staggers if the tuples are same length
		//e.g. 1,2,3 + 4,5 = 1,6,8
		//so this is a bit tricky
		Tuple<String>largerTup;
		Tuple<String>smallerTup;
		if (tup1.size() >= tup2.size()) {
			largerTup = tup1;
			smallerTup = tup2;
		}
		else {
			largerTup = tup2;
			smallerTup = tup1;
		}		
		int gap = largerTup.size() - smallerTup.size();
		
		for (int i=0; i< largerTup.size();i++) {
			if (i < gap) {
				outputTup.add(largerTup.getElement(i));
			}
			else {
				double elementSum = 0;
				elementSum = Double.parseDouble(largerTup.getElement(i)) +
						Double.parseDouble(smallerTup.getElement(i-gap));
				outputTup.add(String.valueOf(elementSum));
			}
		}
		return outputTup;
	}
	
	public abstract List<Iterator<Tuple<String>>> execute(List<Iterator<Tuple<String>>> input);
}

