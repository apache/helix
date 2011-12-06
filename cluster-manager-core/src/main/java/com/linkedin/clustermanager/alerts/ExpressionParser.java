package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;

public class ExpressionParser {
	private static Logger logger = Logger.getLogger(ExpressionParser.class);
	
	final static String opDelim = "|";
	final static String argDelim = ",";
	final static String statFieldDelim = ".";
	final static String wildcardChar = "*";
	
	//static Map<String, ExpressionOperatorType> operatorMap = new HashMap<String, ExpressionOperatorType>();
	
	static Map<String, Operator> operatorMap = new HashMap<String, Operator>();
	static Map<String, Aggregator> aggregatorMap = new HashMap<String, Aggregator>();
	
	
	  static
	  {
		  
		  addOperatorEntry("EXPAND", new ExpandOperator());
		  
		  addAggregatorEntry("ACCUMULATE", new AccumulateAggregator());
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
	  
	  public static Operator getOperatorType(String expression) throws Exception
	  {
		  String op = expression.substring(0,expression.indexOf("("));
		  if (!operatorMap.containsKey(op)) {
			  throw new Exception(op+" is not a valid op type");
		  }
		  return operatorMap.get(op);
	  }
	  
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
	  
	  public static void validateOperators(String expression) throws Exception
	  {
		  
	  }
	   
	  /*
	   * Validate 2 sets of parenthesis exist, all before first opDelim
	   * 
	   * TODO: extract agg type and validate it exists.  validate number of args passed in
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
	  }
	  
	  /*
	   * Return true if stat name matches exactly...incomingStat has no agg type currentStat can have any
	   */
	  public static boolean isExactMatch(String currentStat, String incomingStat)
	  {
		  String currentStatName = getSingleAggregatorStat(currentStat);
		  currentStatName = currentStatName.replaceAll("\\s+", ""); //remove white space
		  String incomingStatName = incomingStat.replaceAll("\\s+", ""); //remove whitespace
		  return (incomingStatName.equals(currentStatName));
	  }
	  /*
	   * Return true if incomingStat matches wildcardStat except currentStat has 1+ fields with "*"
	   * a*.c* matches a5.c7    a*.c* does not match a5.b6.c7
	   */
	  public static boolean isWildcardMatch(String currentStat, String incomingStat)
	  {
		  //TODO: implement this
		  String currentStatName = getSingleAggregatorStat(currentStat);
		  currentStatName = currentStatName.replaceAll("\\s+", ""); //remove white space
		  String incomingStatName = incomingStat.replaceAll("\\s+", ""); //remove whitespace
		  
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
				  String currTokPreWildcard = currTok.substring(0,currTok.length()-1);
				  Pattern pattern = Pattern.compile(currTokPreWildcard+".+");  //form pattern...wildcard part can be anything
				  Matcher matcher = pattern.matcher(incomingTok); //see if incomingTok matches
				  if (!matcher.find()) { //no match on one tok, return false
					  return false;
				  }
			  }
		  }
		  //all fields match or wildcard match...return true!
		  return true;
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
		  String[] statList = (expression.substring(expression.lastIndexOf("(")+1, expression.lastIndexOf(")"))).split(argDelim);
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
		  
		  
		  //String[] opNames = null;
		  
		  
		  
		  
		  //return null;
		  
		  //confirm this is a true agg type
		  //parse out args for agg type and the stat names.
		  //generate the new stat names.
		  
		  //XXX: how do I know number of args
		  
		  /*
		  String[] statNames = null;
		  String statList = expression;
		  String[] opNames = null;
		  //parse apart stats from ops
		  if (statList.contains(opDelim)) {
			  statList = expression.substring(0,expression.indexOf(opDelim));
			  logger.debug("statList: "+statList);
			  String opList = expression.substring(expression.indexOf(opDelim)+1);
			  logger.debug("opList: "+opList);
			  String splitter = "\\"+opDelim;
			  opNames = opList.split(splitter);
		  }
		  if (statList.contains("(")) {
			  statList = statList.substring(expression.indexOf("(")+1, expression.indexOf(")"));
		  }
		  statNames = statList.split(statDelim);
		  
		  if (opNames == null) {  //done, no need to take on op names
			  return statNames;
		  }

		  //build new set of ops, but only base ones, add to each stat
		  StringBuilder baseOnlyOps = new StringBuilder();
		  logger.debug("opNames size: "+opNames.length);
		  for (String op : opNames) {
			  logger.debug("op: "+op);
			  op = op.toUpperCase();
			  if (!operatorMap.containsKey(op)) {
				  throw new Exception(op + " not a valid op type");
			  }
			  if (operatorMap.get(op).isBaseOp()) {
				  baseOnlyOps.append(opDelim);
				  baseOnlyOps.append(op);
			  }
		  }
		  String baseOpsList = baseOnlyOps.toString();
		  for (int i=0; i<statNames.length; i++) {
			  statNames[i] = statNames[i].concat(baseOpsList);
		  }
		  return statNames;
		  */
	  }
	  
	  
	  /*
	  //1) parse expression to get args...picking out key words is open problem
	   //need to parse to first "(" and to last ")".  then parse on comma...without going deeper into "("
	  //2) check number of args, and expression against templatemap and make sure there is entry..else error
	  //3) this perhaps need to be done recursively...in fact, this function should return next level args, 
	  //which may themselves be expressions
	public static String[] getArgs(String expression) 
	{
		String type = null; //get outer most thing
		
		
		String template = null;
	    if (templateMap.containsKey(type))
	    {
	      template = templateMap.get(type).get(keys.length);
	    }

	    String result[] = new String[1];
	    
	    if (template != null)
	    {
	    }
	    result[0] = template;
	    return result;
	}
	*/
}
