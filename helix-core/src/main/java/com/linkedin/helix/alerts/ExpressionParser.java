package com.linkedin.helix.alerts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.linkedin.helix.ClusterManagerException;

public class ExpressionParser {
	private static Logger logger = Logger.getLogger(ExpressionParser.class);
	
	final static String opDelim = "|";
	final static String opDelimForSplit = "\\|";
	final static String argDelim = ",";
	final public static String statFieldDelim = ".";
	final static String wildcardChar = "*";
	
	//static Map<String, ExpressionOperatorType> operatorMap = new HashMap<String, ExpressionOperatorType>();
	
	static Map<String, Operator> operatorMap = new HashMap<String, Operator>();
	static Map<String, Aggregator> aggregatorMap = new HashMap<String, Aggregator>();
	
	
	  static
	  {
		  
		  addOperatorEntry("EXPAND", new ExpandOperator());
		  addOperatorEntry("DIVIDE", new DivideOperator());
		  addOperatorEntry("SUM", new SumOperator());
		  addOperatorEntry("SUMEACH", new SumEachOperator());
		  
		  addAggregatorEntry("ACCUMULATE", new AccumulateAggregator());
		  addAggregatorEntry("DECAY", new DecayAggregator());
		  addAggregatorEntry("WINDOW", new WindowAggregator());
		  /*
		  addEntry("EACH", ExpressionOperatorType.EACH);
		  addEntry("SUM", ExpressionOperatorType.SUM);
		  addEntry("DIVIDE", ExpressionOperatorType.DIVIDE);
		  addEntry("ACCUMULATE", ExpressionOperatorType.ACCUMULATE);
		  */
	  }
	  
	  //static Pattern pattern = Pattern.compile("(\\{.+?\\})");
	  
	  private static void addOperatorEntry(String label, Operator op)
	  {
		  if (!operatorMap.containsKey(label))
		    {
		      operatorMap.put(label, op);
		    }
		    logger.info("Adding operator: "+op);
	  }
	  
	  private static void addAggregatorEntry(String label, Aggregator agg)
	  {
		  if (!aggregatorMap.containsKey(label.toUpperCase()))
		    {
		      aggregatorMap.put(label.toUpperCase(), agg);
		    }
		    logger.info("Adding aggregator: "+agg);
	  }
	  
	  /*
	  private static void addEntry(String label, ExpressionOperatorType type)
	  {
	    if (!operatorMap.containsKey(label))
	    {
	      operatorMap.put(label, type);
	    }
	    logger.info("Adding operator type: "+type);
	  }
	  */
	  
	  public static boolean isExpressionNested(String expression) {
		  return expression.contains("(");
	  }
	  
	  /*
	  public static Operator getOperatorType(String expression) throws Exception
	  {
		  String op = expression.substring(0,expression.indexOf("("));
		  if (!operatorMap.containsKey(op)) {
			  throw new Exception(op+" is not a valid op type");
		  }
		  return operatorMap.get(op);
	  }
	  */
	  
	  public static String getInnerExpression(String expression) 
	  {
		  return expression.substring(expression.indexOf("(")+1,expression.lastIndexOf(")"));
	  }
	  
	  /*
	  public static String[] getBaseStats(ExpressionOperatorType type, String expression) throws Exception
	  {
		  String[] items = null;
		  if (isExpressionNested(expression)) {
			  ExpressionOperatorType nextType = getOperatorType(expression);
				String innerExp = getInnerExpression(expression);
				items = getBaseStats(nextType, innerExp);
		  }
		  else { //base class, no nesting
			  items = expression.split(",");
		  }
		  
		  if (type != null && type.isBaseOp()) { //surround items with type.
			  for (int i=0; i<items.length; i++) {
				  items[i] = type + "(" + items[i] + ")"; //!!!! NEED type to behave like string here 
				  logger.debug("Forming item "+items[i]);
			  }
		  }
		  return items;
	  }
	  
	  public static String[] getBaseStats(String expression) throws Exception
	  {
		expression = expression.replaceAll("\\s+", "");
		return getBaseStats(null, expression);
	  }
	  */
	  
	  /*
	   * Validate 2 sets of parenthesis exist, all before first opDelim
	   * 
	   * TODO: extract agg type and validate it exists.  validate number of args passed in
	   * 
	   */
	  public static void validateAggregatorFormat(String expression) throws ClusterManagerException
	  {
		  logger.debug("validating aggregator for expression: "+expression);
		  //have 0 or more args, 1 or more stats...e.g. ()(x) or (2)(x,y)
		  Pattern pattern = Pattern.compile("\\(.*?\\)\\(.+?\\)");
		  Matcher matcher = pattern.matcher(expression);
		  if (!matcher.find()) {
			  throw new ClusterManagerException(expression +" does not have correct parenthesis");
		  }
		  logger.debug("found valid aggregator "+matcher.group());
		  int patternEnd = matcher.end();
		  if (expression.length() > patternEnd+1) { //if more, check for what follows
		  if (expression.substring(patternEnd+1).contains("(") || 
		  	expression.substring(patternEnd+1).contains(")")) {
			  	throw new ClusterManagerException(expression +" has extra parenthesis");
		  	}
		  }
		  
		  //check wildcard locations.  each part can have at most 1 wildcard, and must be at end
		  StringTokenizer fieldTok = new StringTokenizer(expression, statFieldDelim);
		  while (fieldTok.hasMoreTokens()) {
			  String currTok = fieldTok.nextToken();
			  if (currTok.contains(wildcardChar)) {
				  if (currTok.indexOf(wildcardChar) != currTok.length()-1 ||
						  currTok.lastIndexOf(wildcardChar) != currTok.length()-1) {
					  throw new ClusterManagerException(currTok+" is illegal stat name.  Single wildcard must appear at end.");
				  }
			  }
		  }
	  }
	  
	  public static boolean statContainsWildcards(String stat)
	  {
		  return stat.contains(wildcardChar);
	  }
	  
	  /*
	   * Return true if stat name matches exactly...incomingStat has no agg type currentStat can have any
	   * 
	   * Function can match for 2 cases
	   * extractStatFromAgg=false.  Match accumulate()(dbFoo.partition10.latency) with accumulate()(dbFoo.partition10.latency)...trival
	   * extractStatFromAgg=true.  Match accumulate()(dbFoo.partition10.latency) with dbFoo.partition10.latency
	   * 
	   */
	  public static boolean isExactMatch(String currentStat, String incomingStat, boolean extractStatFromAgg)
	  {
		  String currentStatName = currentStat;
		  if (extractStatFromAgg) {
			  currentStatName = getSingleAggregatorStat(currentStat);
		  }
		  currentStatName = currentStatName.replaceAll("\\s+", ""); //remove white space
		  String incomingStatName = incomingStat.replaceAll("\\s+", ""); //remove whitespace
		  return (incomingStatName.equals(currentStatName));
	  }
	  /*
	   * Return true if incomingStat matches wildcardStat except currentStat has 1+ fields with "*"
	   * a*.c* matches a5.c7    a*.c* does not match a5.b6.c7
	   * 
	   * Function can match for 2 cases
	   * extractStatFromAgg=false.  Match accumulate()(dbFoo.partition*.latency) with accumulate()(dbFoo.partition10.latency)
	   * extractStatFromAgg=true.  Match accumulate()(dbFoo.partition*.latency) with dbFoo.partition10.latency
	   */
	  public static boolean isWildcardMatch(String currentStat, String incomingStat, boolean statCompareOnly, ArrayList<String> bindings)
	  {
		  if (!statCompareOnly) { //need to check for match on agg type and stat
			  String currentStatAggType = (currentStat.split("\\)"))[0];
			  String incomingStatAggType = (incomingStat.split("\\)"))[0];
			  if (!currentStatAggType.equals(incomingStatAggType)) {
				  return false;
			  }
		  }
		  //now just get the stats
		  String currentStatName = getSingleAggregatorStat(currentStat);
		  String incomingStatName = getSingleAggregatorStat(incomingStat);
		  
		  currentStatName = currentStatName.replaceAll("\\s+", ""); //remove white space
		  incomingStatName = incomingStatName.replaceAll("\\s+", ""); //remove whitespace
		  
		  if (! currentStatName.contains(wildcardChar)) { //no wildcards in stat name
			  return false;
		  }
		  StringTokenizer currentStatTok = new StringTokenizer(currentStatName, statFieldDelim);
		  StringTokenizer incomingStatTok = new StringTokenizer(incomingStatName, statFieldDelim);
		  if (currentStatTok.countTokens() != incomingStatTok.countTokens()) { //stat names different numbers of fields
			  return false;
		  }
		  //for each token, if not wildcarded, must be an exact match
		  while (currentStatTok.hasMoreTokens()) {
			  String currTok = currentStatTok.nextToken();
			  String incomingTok = incomingStatTok.nextToken();
			  logger.debug("curTok: "+currTok);
			  logger.debug("incomingTok: "+incomingTok);
			  if (!currTok.contains(wildcardChar)) { //no wildcard, but have exact match
				  if (!currTok.equals(incomingTok)) { //not exact match
					  return false;
				  }
			  }
			  else { //currTok has a wildcard
				  if (currTok.indexOf(wildcardChar) != currTok.length()-1 ||
						  currTok.lastIndexOf(wildcardChar) != currTok.length()-1) {
					  throw new ClusterManagerException(currTok+" is illegal stat name.  Single wildcard must appear at end.");
				  }
				  //for wildcard matching, need to escape parentheses on currTok, so regex works
				  //currTok = currTok.replace("(", "\\(");
				  //currTok = currTok.replace(")", "\\)");
				  //incomingTok = incomingTok.replace("(", "\\(");
				  //incomingTok = incomingTok.replace(")", "\\)");
				  String currTokPreWildcard = currTok.substring(0,currTok.length()-1);
				  //TODO: if current token has a "(" in it, pattern compiling throws error
				  //Pattern pattern = Pattern.compile(currTokPreWildcard+".+");  //form pattern...wildcard part can be anything
				  //Matcher matcher = pattern.matcher(incomingTok); //see if incomingTok matches
				  if (incomingTok.indexOf(currTokPreWildcard) != 0) {
				  //if (!matcher.find()) { //no match on one tok, return false
					  return false;
				  }
				  //get the binding
				  
				  if (bindings != null) {
					  //TODO: debug me!
					  String wildcardBinding = incomingTok.substring(incomingTok.indexOf(currTokPreWildcard)+currTokPreWildcard.length());
					  bindings.add(wildcardBinding);
				  }
			  }
		  }
		  //all fields match or wildcard match...return true!
		  return true;
	  }
	  
	  /*
	   * For checking if an incoming stat (no agg type defined) matches a persisted stat (with agg type defined)
	   */
	  public static boolean isIncomingStatExactMatch(String currentStat, String incomingStat)
	  {
		  return isExactMatch(currentStat, incomingStat, true);
	  }
	  
	  /*
	   * For checking if an incoming stat (no agg type defined) wildcard matches a persisted stat (with agg type defined)
	   * The persisted stat may have wildcards
	   */
	  public static boolean isIncomingStatWildcardMatch(String currentStat, String incomingStat)
	  {
		  return isWildcardMatch(currentStat, incomingStat, true, null);
	  }
	  
	  /*
	   * For checking if a persisted stat matches a stat defined in an alert
	   */
	  public static boolean isAlertStatExactMatch(String alertStat, String currentStat)
	  {
		  return isExactMatch(alertStat, currentStat, false);
	  }
	  
	  /*
	   * For checking if a maintained stat wildcard matches a stat defined in an alert.  The alert may have wildcards
	   */
	  public static boolean isAlertStatWildcardMatch(String alertStat, String currentStat, ArrayList<String> wildcardBindings)
	  {
		  return isWildcardMatch(alertStat, currentStat, false, wildcardBindings);
	  }
	 
	  public static Aggregator getAggregator(String aggStr) throws ClusterManagerException
	  {
		  aggStr = aggStr.toUpperCase();
		  Aggregator agg = aggregatorMap.get(aggStr);
		  if (agg == null) {
			  throw new ClusterManagerException("Unknown aggregator type "+aggStr);
		  }
		  return agg;
	  }
	  
	  public static String getAggregatorStr(String expression) throws ClusterManagerException 
	  {
		  if (!expression.contains("(")) {
			  throw new ClusterManagerException(expression+" does not contain a valid aggregator.  No parentheses found");
		  }
		  String aggName = expression.substring(0,expression.indexOf("("));
		  if (!aggregatorMap.containsKey(aggName.toUpperCase())) {
			  throw new ClusterManagerException("aggregator <"+aggName+"> is unknown type");
		  }
		  return aggName;
	  }
	  
	  public static String[] getAggregatorArgs(String expression) throws ClusterManagerException
	  {
		  String aggregator = getAggregatorStr(expression);
		  String argsStr = getAggregatorArgsStr(expression);
		  String[] args = argsStr.split(argDelim);
		  logger.debug("args size: "+args.length);
		  int numArgs = (argsStr.length() == 0) ? 0 : args.length; 
		  //String[] argList = (expression.substring(expression.indexOf("(")+1, expression.indexOf(")"))).split(argDelim);
		  //verify correct number of args
		  int requiredNumArgs = aggregatorMap.get(aggregator.toUpperCase()).getRequiredNumArgs();
		  if (numArgs != requiredNumArgs)
		  {
			  throw new ClusterManagerException(
					  expression+" contains "+args.length+" arguments, but requires "+requiredNumArgs);
		  }
		  return args;
	  }
	  
	  /*
	  public static String[] getAggregatorArgsList(String expression) {
		  String argsStr = getAggregatorArgsStr(expression);
		  String[] args = argsStr.split(argDelim);
		  return args;
	  }
	  */
	  
	  public static String getAggregatorArgsStr(String expression)
	  {
		  return expression.substring(expression.indexOf("(")+1, expression.indexOf(")"));
	  }
	  
	  public static String[] getAggregatorStats(String expression) throws ClusterManagerException
	  {
		  String justStats = expression;
		  if (expression.contains("(") && expression.contains(")")) {
			  justStats = (expression.substring(expression.lastIndexOf("(")+1, expression.lastIndexOf(")")));
		  }
		  String[] statList = justStats.split(argDelim);
		  if (statList.length < 1) {
			  throw new ClusterManagerException(expression+" does not contain any aggregator stats");
		  }
		  return statList;
	  }
	  
	  public static String getSingleAggregatorStat(String expression) throws ClusterManagerException
	  {
		  
		  String[] stats = getAggregatorStats(expression);
		  if (stats.length > 1) {
			  throw new ClusterManagerException(expression+" contains more than 1 stat");
		  }
		  return stats[0];
	  }
	  
	  public static String getWildcardStatSubstitution(String wildcardStat, String fixedStat)
	  {
		  int lastOpenParenLoc = wildcardStat.lastIndexOf("(");
		  int lastCloseParenLoc = wildcardStat.lastIndexOf(")");
		  StringBuilder builder = new StringBuilder();
		  builder.append(wildcardStat.substring(0,lastOpenParenLoc+1));
		  builder.append(fixedStat);
		  builder.append(")");
		  logger.debug("wildcardStat: "+wildcardStat);
		  logger.debug("fixedStat: "+fixedStat);
		  logger.debug("subbedStat: "+builder.toString());
		  return builder.toString();
	  }
	  
	  //XXX: each op type should have number of inputs, number of outputs.  do validation.
	  //(dbFoo.partition*.latency, dbFoo.partition*.count)|EACH|ACCUMULATE|DIVIDE
	  public static String[] getBaseStats(String expression) throws ClusterManagerException
	  {
		  expression = expression.replaceAll("\\s+", ""); //remove white space
		  
		  validateAggregatorFormat(expression);
			
		  String aggName = getAggregatorStr(expression);
		  String[] aggArgs = getAggregatorArgs(expression);
		  String[] aggStats = getAggregatorStats(expression);
		  
		  //form aggArgs
		  String aggArgList = getAggregatorArgsStr(expression);
		  
		  String[] baseStats = new String[aggStats.length];
		  for (int i=0; i<aggStats.length;i++) {
			  StringBuilder stat = new StringBuilder();
			  stat.append(aggName);
			  stat.append("(");
			  stat.append(aggArgList);
			  stat.append(")");
			  stat.append("(");
			  stat.append(aggStats[i]);
			  stat.append(")");
			  baseStats[i] = stat.toString();
		  }
		  return baseStats;
	  }
	  
	  public static String[] getOperators(String expression) throws ClusterManagerException
	  {
		  String[] ops = null;
		  int numAggStats = (getAggregatorStats(expression)).length;
		  int opDelimLoc = expression.indexOf(opDelim);
		  if (opDelimLoc < 0) {
			  return null;
		  }
		  logger.debug("ops str: "+expression.substring(opDelimLoc+1));
		  ops = expression.substring(opDelimLoc+1).split(opDelimForSplit);
		 
		  //validate this string of ops 
		  //verify each op exists
		  //take num input tuples sets and verify ops will output exactly 1 tuple sets
		  int currNumTuples = numAggStats;
		  for (String op : ops) {
			  logger.debug("op: "+op);
			  if (!operatorMap.containsKey(op.toUpperCase())) {
				  throw new ClusterManagerException("<"+op+"> is not a valid operator type");
			  }
			  Operator currOpType = operatorMap.get(op.toUpperCase());
			  if (currNumTuples < currOpType.minInputTupleLists || currNumTuples > currOpType.maxInputTupleLists) {
				  throw new ClusterManagerException("<"+op+"> cannot process "+currNumTuples+" input tuples");
			  }
			  //reset num tuples to this op's output size
			  if (! currOpType.inputOutputTupleListsCountsEqual) { //if equal, this number does not change
				  currNumTuples = currOpType.numOutputTupleLists;
			  }
		  }
		  if (currNumTuples != 1) {
			  throw new ClusterManagerException(expression+" does not terminate in a single tuple set");
		  }
		  return ops;
	  }
	  
	  public static void validateOperators(String expression) throws ClusterManagerException
	  {
		  getOperators(expression);
	  }
	  
	  public static Operator getOperator(String opName) throws ClusterManagerException
	  {
		  opName = opName.replaceAll("\\s+", ""); //remove white space
		  if (!operatorMap.containsKey(opName)) {
			 throw new ClusterManagerException(opName + " is unknown op type");
		  }
		  return operatorMap.get(opName);
	  }
	  
	  public static void validateExpression(String expression) throws ClusterManagerException
	  {
		  //1. extract stats part and validate
		  validateAggregatorFormat(expression);
		  //2. extract ops part and validate the ops exist and the inputs/outputs are correct
		  validateOperators(expression);
	  }
}
