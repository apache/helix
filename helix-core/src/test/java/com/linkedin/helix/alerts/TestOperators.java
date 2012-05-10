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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.helix.alerts.SumEachOperator;
import com.linkedin.helix.alerts.SumOperator;
import com.linkedin.helix.alerts.Tuple;


public class TestOperators {
	
	SumOperator _sumOp;
	SumEachOperator _sumEachOp;
	
	@BeforeMethod (groups = {"unitTest"})
	public void setup()
	{
		_sumOp = new SumOperator();
		_sumEachOp = new SumEachOperator();
	}
	
	
	@Test (groups = {"unitTest"})
	public void testTwoNulls()
	{
		Tuple<String> tup1 = null;
		Tuple<String> tup2 = null;
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals(null, resultTup);
	}
	
	@Test (groups = {"unitTest"})
	public void testOneNullLeft()
	{
		Tuple<String> tup1 = null;
		Tuple<String> tup2 = new Tuple<String>();
		tup2.add("1.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals("1.0", resultTup.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testOneNullRight()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = null;
		tup1.add("1.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals("1.0", resultTup.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testTwoSingeltons()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = new Tuple<String>();
		tup1.add("1.0");
		tup2.add("2.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals("3.0", resultTup.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testThreeSingeltons()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = new Tuple<String>();
		Tuple<String> tup3 = new Tuple<String>();
		tup1.add("1.0");
		tup2.add("2.0");
		tup3.add("3.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup3List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		tup3List.add(tup3);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		tupsList.add(tup3List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals("6.0", resultTup.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testThreeTriples()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = new Tuple<String>();
		Tuple<String> tup3 = new Tuple<String>();
		tup1.add("1.0"); tup1.add("2.0"); tup1.add("3.0");
		tup2.add("4.0"); tup2.add("5.0"); tup2.add("6.0");
		tup3.add("7.0"); tup3.add("8.0"); tup3.add("9.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup3List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		tup3List.add(tup3);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		tupsList.add(tup3List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		AssertJUnit.assertEquals("12.0,15.0,18.0", resultTup.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testThreeTriplesOneMissing()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = new Tuple<String>();
		Tuple<String> tup3 = new Tuple<String>();
		tup1.add("1.0"); tup1.add("2.0"); tup1.add("3.0");
		tup2.add("5.0"); tup2.add("6.0");
		tup3.add("7.0"); tup3.add("8.0"); tup3.add("9.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup3List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1);
		tup2List.add(tup2);
		tup3List.add(tup3);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		tupsList.add(tup3List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup = resultIter.next();
		//tuple 2 missing 1 entry, other 2 get bumped to right
		AssertJUnit.assertEquals("8.0,15.0,18.0", resultTup.toString());
	}

	//test multiple rows
	@Test (groups = {"unitTest"})
	public void testThreeTriplesOneMissingTwoRows()
	{
		Tuple<String> tup1Dot1 = new Tuple<String>();
		Tuple<String> tup2Dot1 = new Tuple<String>();
		Tuple<String> tup3Dot1 = new Tuple<String>();
		Tuple<String> tup1Dot2 = new Tuple<String>();
		Tuple<String> tup2Dot2 = new Tuple<String>();
		Tuple<String> tup3Dot2 = new Tuple<String>();
		tup1Dot1.add("1.0"); tup1Dot1.add("2.0"); tup1Dot1.add("3.0");
		tup2Dot1.add("5.0"); tup2Dot1.add("6.0");
		tup3Dot1.add("7.0"); tup3Dot1.add("8.0"); tup3Dot1.add("9.0");
		tup1Dot2.add("10.0"); tup1Dot2.add("11.0"); tup1Dot2.add("12.0");
		tup2Dot2.add("13.0"); tup2Dot2.add("14.0"); tup2Dot2.add("15.0");
		tup3Dot2.add("16.0"); tup3Dot2.add("17.0"); tup3Dot2.add("18.0");
		List<Tuple<String>> tup1List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup2List = new ArrayList<Tuple<String>>();
		List<Tuple<String>> tup3List = new ArrayList<Tuple<String>>();
		tup1List.add(tup1Dot1);
		tup2List.add(tup2Dot1);
		tup3List.add(tup3Dot1);
		tup1List.add(tup1Dot2);
		tup2List.add(tup2Dot2);
		tup3List.add(tup3Dot2);
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(tup1List.iterator());
		tupsList.add(tup2List.iterator());
		tupsList.add(tup3List.iterator());
		List<Iterator<Tuple<String>>> result = _sumOp.execute(tupsList);
		AssertJUnit.assertEquals(1, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup1 = resultIter.next();
		//tuple 2 missing 1 entry, other 2 get bumped to right
		AssertJUnit.assertEquals("8.0,15.0,18.0", resultTup1.toString());
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup2 = resultIter.next();
		AssertJUnit.assertEquals("39.0,42.0,45.0", resultTup2.toString());
	}
	
	@Test (groups = {"unitTest"})
	public void testSumAll()
	{
		Tuple<String> tup1 = new Tuple<String>();
		Tuple<String> tup2 = new Tuple<String>();
		Tuple<String> tup3 = new Tuple<String>();
		Tuple<String> tup4 = new Tuple<String>();
		Tuple<String> tup5 = new Tuple<String>();
		Tuple<String> tup6 = new Tuple<String>();
		tup1.add("1.0"); tup2.add("2.0"); tup3.add("3.0");
		tup4.add("4.0"); tup5.add("5.0"); tup6.add("6.0");
		List<Tuple<String>> list1 = new ArrayList<Tuple<String>>();
		List<Tuple<String>> list2 = new ArrayList<Tuple<String>>();
		List<Tuple<String>> list3 = new ArrayList<Tuple<String>>();
		list1.add(tup1); list1.add(tup4); 
		list2.add(tup2); list2.add(tup5);
		list3.add(tup3); list3.add(tup6);
		
		List<Iterator<Tuple<String>>> tupsList = new ArrayList<Iterator<Tuple<String>>>();
		tupsList.add(list1.iterator());
		tupsList.add(list2.iterator());
		tupsList.add(list3.iterator());
		List<Iterator<Tuple<String>>> result = _sumEachOp.execute(tupsList);
		AssertJUnit.assertEquals(3, result.size()); //should be just 1 iter
		Iterator<Tuple<String>> resultIter = result.get(0);
		AssertJUnit.assertTrue(resultIter.hasNext());
		Tuple<String>resultTup1 = resultIter.next();
		AssertJUnit.assertEquals("5.0", resultTup1.toString());
		resultIter = result.get(1);
		AssertJUnit.assertTrue(resultIter.hasNext());
		resultTup1 = resultIter.next();
		AssertJUnit.assertEquals("7.0", resultTup1.toString());
		resultIter = result.get(2);
		AssertJUnit.assertTrue(resultIter.hasNext());
		resultTup1 = resultIter.next();
		AssertJUnit.assertEquals("9.0", resultTup1.toString());
	}
}
