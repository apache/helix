package com.linkedin.helix.alerts;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.alerts.AlertParser;

@Test
public class TestAlertValidation {

	public final String EXP = AlertParser.EXPRESSION_NAME;
	public final String CMP = AlertParser.COMPARATOR_NAME;
	public final String CON = AlertParser.CONSTANT_NAME;

	@Test
	public void testSimple() {
		String alertName = EXP + "(accumulate()(dbFoo.partition10.latency)) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertFalse(caughtException);
	}
	
	@Test
	public void testSingleInSingleOut() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition10.latency)|EXPAND) " + CMP
				+ "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertFalse(caughtException);
	}

	@Test
	public void testDoubleInDoubleOut() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition10.latency, dbFoo.partition11.latency)|EXPAND) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test
	public void testTwoStageOps() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EXPAND|DIVIDE) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertFalse(caughtException);
	}

	@Test
	public void testTwoListsIntoOne() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition10.latency, dbFoo.partition11.count)|SUM) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertFalse(caughtException);
	}

	@Test
	public void testSumEach() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition*.latency, dbFoo.partition*.count)|EXPAND|SUMEACH|DIVIDE) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertFalse(caughtException);
	}

	@Test
	public void testNeedTwoTuplesGetOne() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition*.latency)|EXPAND|DIVIDE) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test
	public void testExtraPipe() {
		String alertName = EXP + "(accumulate()(dbFoo.partition10.latency)|) "
				+ CMP + "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test
	public void testAlertUnknownOp() {
		String alertName = EXP
				+ "(accumulate()(dbFoo.partition10.latency)|BADOP) " + CMP
				+ "(GREATER) " + CON + "(10)";
		boolean caughtException = false;
		try {
			AlertParser.validateAlert(alertName);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}
}
