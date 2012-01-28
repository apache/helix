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
