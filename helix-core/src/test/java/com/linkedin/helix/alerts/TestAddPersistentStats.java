package com.linkedin.helix.alerts;

import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.Mocks.MockManager;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.stages.HealthDataCache;

public class TestAddPersistentStats {

	protected static final String CLUSTER_NAME = "TestCluster";

	MockManager _helixManager;
	StatsHolder _statsHolder;

	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_helixManager = new MockManager(CLUSTER_NAME);
		_statsHolder = new StatsHolder(_helixManager, new HealthDataCache());
	}

	public boolean statRecordContains(ZNRecord rec, String statName)
	{
		Map<String,Map<String,String>> stats = rec.getMapFields();
		return stats.containsKey(statName);
	}

	public int statsSize(ZNRecord rec)
	{
		Map<String,Map<String,String>> stats = rec.getMapFields();
		return stats.size();
	}

	@Test (groups = {"unitTest"})
	public void testAddStat() throws Exception
	{
		String stat = "window(5)(dbFoo.partition10.latency)";
		_statsHolder.addStat(stat);
		ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		System.out.println("rec: "+rec.toString());
		AssertJUnit.assertTrue(statRecordContains(rec,stat));
		AssertJUnit.assertEquals(1, statsSize(rec));
	}

	@Test (groups = {"unitTest"})
	public void testAddTwoStats() throws Exception
	{
		String stat1 = "window(5)(dbFoo.partition10.latency)";
		_statsHolder.addStat(stat1);
		String stat2 = "window(5)(dbFoo.partition11.latency)";
		_statsHolder.addStat(stat2);
		ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		System.out.println("rec: "+rec.toString());
		AssertJUnit.assertTrue(statRecordContains(rec,stat1));
		AssertJUnit.assertTrue(statRecordContains(rec,stat2));
		AssertJUnit.assertEquals(2, statsSize(rec));
	}

	@Test (groups = {"unitTest"})
	public void testAddDuplicateStat() throws Exception
	{
		String stat = "window(5)(dbFoo.partition10.latency)";
		_statsHolder.addStat(stat);
		_statsHolder.addStat(stat);
		ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		System.out.println("rec: "+rec.toString());
		AssertJUnit.assertTrue(statRecordContains(rec,stat));
		AssertJUnit.assertEquals(1, statsSize(rec));
	}

	@Test (groups = {"unitTest"})
	public void testAddPairOfStats() throws Exception
	{
		String exp = "accumulate()(dbFoo.partition10.latency, dbFoo.partition10.count)";
		_statsHolder.addStat(exp);
		ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		System.out.println("rec: "+rec.toString());
		AssertJUnit.assertTrue(statRecordContains(rec,"accumulate()(dbFoo.partition10.latency)"));
		AssertJUnit.assertTrue(statRecordContains(rec,"accumulate()(dbFoo.partition10.count)"));
		AssertJUnit.assertEquals(2, statsSize(rec));
	}

	@Test (groups = {"unitTest"})
	public void testAddStatsWithOperators() throws Exception
	{
		String exp = "accumulate()(dbFoo.partition10.latency, dbFoo.partition10.count)|EACH|ACCUMULATE|DIVIDE";
		_statsHolder.addStat(exp);
		ZNRecord rec = _helixManager.getDataAccessor().getProperty(PropertyType.PERSISTENTSTATS);
		System.out.println("rec: "+rec.toString());
		AssertJUnit.assertTrue(statRecordContains(rec,"accumulate()(dbFoo.partition10.latency)"));
		AssertJUnit.assertTrue(statRecordContains(rec,"accumulate()(dbFoo.partition10.count)"));
		AssertJUnit.assertEquals(2, statsSize(rec));
	}

	@Test (groups = {"unitTest"})
	public void testAddNonExistentAggregator() throws Exception
	{
		String exp = "fakeagg()(dbFoo.partition10.latency)";
		boolean caughtException = false;
		try {
			_statsHolder.addStat(exp);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test (groups = {"unitTest"})
	public void testGoodAggregatorBadArgs() throws Exception
	{
		String exp = "accumulate(10)(dbFoo.partition10.latency)";
		boolean caughtException = false;
		try {
			_statsHolder.addStat(exp);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test (groups = {"unitTest"})
	public void testAddBadNestingStat1() throws Exception
	{
		String exp = "window((5)(dbFoo.partition10.latency)";
		boolean caughtException = false;
		try {
			_statsHolder.addStat(exp);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}

	@Test (groups = {"unitTest"})
	public void testAddBadNestingStat2() throws Exception
	{
		String exp = "window(5)(dbFoo.partition10.latency))";
		boolean caughtException = false;
		try {
			_statsHolder.addStat(exp);
		} catch (HelixException e) {
			caughtException = true;
			e.printStackTrace();
		}
		AssertJUnit.assertTrue(caughtException);
	}
}
