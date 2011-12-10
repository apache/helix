package com.linkedin.clustermanager.alerts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;
import com.linkedin.clustermanager.healthcheck.StatHealthReportProvider;

public class AlertProcessor {
	private static Logger logger = Logger.getLogger(AlertProcessor.class);
	
	private static final String bindingDelim = ",";
	public static final String noWildcardAlertKey = "*";
	
	StatsHolder _statsHolder;
	//AlertsHolder _alertsHolder;
	
	/*
	public AlertProcessor(StatHealthReportProvider statProvider)
	{
		
	}
	*/
	
	public AlertProcessor(StatsHolder sh)
	{
		_statsHolder = sh;
	}
	
	public static Map<String,List<Tuple<String>>> initAlertStatTuples(Alert alert)
	{
		//get the stats out of the alert
		String[] alertStats = ExpressionParser.getBaseStats(alert.getExpression());
		//init a tuple list for each alert stat
		Map<String,List<Tuple<String>>> alertStatTuples = new HashMap<String,List<Tuple<String>>>();
		for (String currAlertStat : alertStats) {
			List<Tuple<String>> currList = new ArrayList<Tuple<String>>();
			alertStatTuples.put(currAlertStat, currList);
		}
		return alertStatTuples;
	}
	
	/*
	//this function is all messed up!!!
	public static void populateAlertStatTuples(Map<String,List<Tuple<String>>> tupleLists, List<Stat> persistentStats)
	{
		Set<String> alertStatNames = tupleLists.keySet();
		for (Stat persistentStat : persistentStats) {
			//ignore stats with wildcards, they don't have values...they are just there to catch new actual stats
			if (ExpressionParser.statContainsWildcards(persistentStat.getName())) {
				continue;
			}
			Iterator<String> alertStatIter = alertStatNames.iterator();
			while (alertStatIter.hasNext()) {
				String currAlertStat = alertStatIter.next();
				if (ExpressionParser.isAlertStatExactMatch(currAlertStat, persistentStat.getName()) ||
						ExpressionParser.isAlertStatWildcardMatch(currAlertStat, persistentStat.getName())) {
					tupleLists.get(currAlertStat).add(persistentStat.getValue());
				}
			}
		}
	}
	*/
	
	public static String formAlertKey(ArrayList<String>bindings)
	{
		if (bindings.size() == 0) {
			return null;
		}
		StringBuilder alertKey = new StringBuilder();
		boolean emptyKey = true;
		for (String binding : bindings) {
			if (!emptyKey) {
				alertKey.append(bindingDelim);
			}
			alertKey.append(binding);
			emptyKey = false;		
		}
		return alertKey.toString();
	}
	
	public static Map<String, ArrayList<Tuple<String>>> populateAlertStatTuples(String[] alertStats, List<Stat> persistentStats)
	{
		Map<String,ArrayList<Tuple<String>>> tupleSets = new HashMap<String,ArrayList<Tuple<String>>>();
		
		//check each persistentStat, alertStat pair
		for (Stat persistentStat : persistentStats) {
			//ignore stats with wildcards, they don't have values...they are just there to catch new actual stats
			if (ExpressionParser.statContainsWildcards(persistentStat.getName())) {
				continue;
			}
			for (int i=0;i<alertStats.length;i++) {
				String alertStat = alertStats[i];
				ArrayList<String>wildcardBindings = new ArrayList<String>();
				//if match, then proceed.  If the match is wildcard, additionally fill in the wildcard bindings
				if (ExpressionParser.isAlertStatExactMatch(alertStat, persistentStat.getName()) ||
						ExpressionParser.isAlertStatWildcardMatch(alertStat, persistentStat.getName(), wildcardBindings)) {
					String alertKey;
					if (wildcardBindings.size() == 0) {
						alertKey = noWildcardAlertKey;
					}
					else {
						alertKey = formAlertKey(wildcardBindings);
					}
					if (!tupleSets.containsKey(alertKey)) { //don't have an entry for alertKey yet, create one
						ArrayList<Tuple<String>> tuples = new ArrayList<Tuple<String>>(alertStats.length);
						for (int j=0;j<alertStats.length;j++) { //init all entries to null
							tuples.add(j,null);
						}
						tupleSets.put(alertKey, tuples); //add to map
					}
					tupleSets.get(alertKey).set(i, persistentStat.getValue());
				}
			}
		}
		
		//post-processing step to discard any rows with null vals...
		//TODO: decide if this is best thing to do with incomplete rows
		for (String setKey : tupleSets.keySet()) {
			ArrayList<Tuple<String>> tupleSet = tupleSets.get(setKey);
			for (Tuple<String> tup : tupleSet) {
				if (tup == null) {
					tupleSets.remove(setKey);
					break; //move on to next setKey
				}
			}
		}
		
		//convert above to a series of iterators
		
		
		return tupleSets;
	}
	
	public static List<Iterator<Tuple<String>>> convertTupleRowsToTupleColumns(Map<String, ArrayList<Tuple<String>>> tupleMap) 
	{
		//input is a map of key -> list of tuples.  each tuple list is same length
		//output should be a list of iterators.  each column in input becomes iterator in output
		
		ArrayList<ArrayList<Tuple<String>>> columns = new ArrayList<ArrayList<Tuple<String>>>();
		ArrayList<Iterator<Tuple<String>>> columnIters = new ArrayList<Iterator<Tuple<String>>>();
		for (String currStat : tupleMap.keySet()) {
			List<Tuple<String>> currSet = tupleMap.get(currStat);
			for (int i=0;i<currSet.size();i++) {
				if (columns.size() < (i+1)) {
					ArrayList<Tuple<String>> col = new ArrayList<Tuple<String>>();
					columns.add(col);
				}
				columns.get(i).add(currSet.get(i));
			}	
		}
		for (ArrayList<Tuple<String>> al : columns) {
			columnIters.add(al.iterator());
		}
		return columnIters;
		
	}
	
	public static Iterator<Tuple<String>> executeOperatorPipeline(List<Iterator<Tuple<String>>> tupleIters, String[] operators)
	{
		List<Iterator<Tuple<String>>> nextIters = tupleIters;
		if (operators != null) {
			for (String opName : operators) {
				Operator op = ExpressionParser.getOperator(opName);
				nextIters = op.execute(nextIters);
			}
		}
		
		if (nextIters.size() != 1) {
			throw new ClusterManagerException("operator pipeline produced "
					+nextIters.size()+" tuple sets instead of exactly 1");
		}
		
		return nextIters.get(0);
	}
	
	/*
	 * TODO: consider returning actual values, rather than bools.  Could just return the triggered alerts
	 */
	public static ArrayList<Boolean> executeComparator(Iterator<Tuple<String>> tuples, String comparatorName, Tuple<String> constant)
	{
		ArrayList<Boolean> results = new ArrayList<Boolean>();
		AlertComparator cmp = AlertParser.getComparator(comparatorName);
		
		while (tuples.hasNext()) {
			results.add(cmp.evaluate(tuples.next(), constant));
		}
		return results;
		
	}
	
	/*
	public static void executeAlert(Alert alert, List<Stat> stats)
	{
		//init tuple lists and populate them
		Map<String,List<Tuple<String>>> alertStatTupleSets = initAlertStatTuples(alert);
		populateAlertStatTuples(alertStatTupleSets, stats); //TODO: not sure I am being careful enough with sticking stats that match each other in this list!
		//convert to operator friendly format
		List<Iterator<Tuple<String>>> tupleIters = convertTupleSetsToTupleIterators(alertStatTupleSets);
		//get the operators
		String[] operators = ExpressionParser.getOperators(alert.getExpression());
		//do operator pipeline
		Iterator<Tuple<String>> opResultTuples = executeOperatorPipeline(tupleIters, operators);
		//execute comparator for tuple list
		ArrayList<Boolean> evalResults = executeComparator(opResultTuples, alert.getComparator(), alert.getConstant()); 
		
		//TODO: convey this back to execute all
		
	}
	*/
	
	public static HashMap<String, Boolean> generateResultMap(Set<String> alertStatBindings ,ArrayList<Boolean> evalResults)
	{
		HashMap<String, Boolean> resultMap = new HashMap<String,Boolean>();
		Iterator<String> bindingIter = alertStatBindings.iterator();
		Iterator<Boolean> resultIter = evalResults.iterator();
		if (alertStatBindings.size() != evalResults.size()) {
			//can't match up alerts bindings to results
			while (resultIter.hasNext()) {
				resultMap.put(noWildcardAlertKey, resultIter.next());
			}
		}
		else {
			//they do match up
			while (resultIter.hasNext()) {
				resultMap.put(bindingIter.next(), resultIter.next());
			}
		}
		return resultMap;
	}
	
	public static HashMap<String, Boolean> executeAlert(Alert alert, List<Stat> persistedStats)
	{
		//init tuple lists and populate them
		//Map<String,List<Tuple<String>>> alertStatTupleSets = initAlertStatTuples(alert);
		
		String[] alertStats = ExpressionParser.getBaseStats(alert.getExpression());
		
		Map<String, ArrayList<Tuple<String>>> alertsToTupleRows = populateAlertStatTuples(alertStats, persistedStats); //TODO: not sure I am being careful enough with sticking stats that match each other in this list!
		
		if (alertsToTupleRows.size() == 0) {
			return null;
		}
		//convert to operator friendly format
		List<Iterator<Tuple<String>>> tupleIters = convertTupleRowsToTupleColumns(alertsToTupleRows);
		//get the operators
		String[] operators = ExpressionParser.getOperators(alert.getExpression());
		//do operator pipeline
		Iterator<Tuple<String>> opResultTuples = executeOperatorPipeline(tupleIters, operators);
		//execute comparator for tuple list
		ArrayList<Boolean> evalResults = executeComparator(opResultTuples, alert.getComparator(), alert.getConstant()); 
		
		//stitch alert bindings back together with final result
		//XXX: need to verify that processing is order preserving here!
		HashMap<String, Boolean> alertBindingsToResult = generateResultMap(alertsToTupleRows.keySet(), evalResults);
		
		return alertBindingsToResult;
		
		
	}
	
	public static Map<String, Map<String, Boolean>> executeAllAlerts(List<Alert> alerts, List<Stat> stats)
	{
		Map<String, Map<String, Boolean>> alertsResults = new HashMap<String, Map<String,Boolean>>();
		
		for (Alert alert : alerts) {
			HashMap<String, Boolean> result = executeAlert(alert, stats);
			//TODO: decide if sticking null results in here is ok
			alertsResults.put(alert.getName(), result);
		}
		
		return alertsResults;
	}
}
