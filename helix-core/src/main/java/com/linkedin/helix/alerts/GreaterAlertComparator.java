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

public class GreaterAlertComparator extends AlertComparator {

	@Override
	/*
	 * Returns true if any element left tuple exceeds any element in right tuple
	 */
	public boolean evaluate(Tuple<String> leftTup, Tuple<String> rightTup) {
		Iterator<String> leftIter = leftTup.iterator();
		while (leftIter.hasNext()) {
			double leftVal = Double.parseDouble(leftIter.next());
			Iterator<String> rightIter = rightTup.iterator();
			while (rightIter.hasNext()) {
				double rightVal = Double.parseDouble(rightIter.next());
				if (leftVal > rightVal) {
					return true;
				}
			}
		}
		return false;
	}

}
