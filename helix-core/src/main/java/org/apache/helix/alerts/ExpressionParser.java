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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.helix.HelixException;
import org.apache.log4j.Logger;

public class ExpressionParser {
  private static Logger logger = Logger.getLogger(ExpressionParser.class);

  final static String opDelim = "|";
  final static String opDelimForSplit = "\\|";
  final static String argDelim = ",";
  final public static String statFieldDelim = ".";
  final static String wildcardChar = "*";

  // static Map<String, ExpressionOperatorType> operatorMap = new
  // HashMap<String, ExpressionOperatorType>();

  static Map<String, Operator> operatorMap = new HashMap<String, Operator>();
  static Map<String, Aggregator> aggregatorMap = new HashMap<String, Aggregator>();

  static {

    addOperatorEntry("EXPAND", new ExpandOperator());
    addOperatorEntry("DIVIDE", new DivideOperator());
    addOperatorEntry("SUM", new SumOperator());
    addOperatorEntry("SUMEACH", new SumEachOperator());

    addAggregatorEntry("ACCUMULATE", new AccumulateAggregator());
    addAggregatorEntry("DECAY", new DecayAggregator());
    addAggregatorEntry("WINDOW", new WindowAggregator());
    /*
     * addEntry("EACH", ExpressionOperatorType.EACH); addEntry("SUM",
     * ExpressionOperatorType.SUM); addEntry("DIVIDE",
     * ExpressionOperatorType.DIVIDE); addEntry("ACCUMULATE",
     * ExpressionOperatorType.ACCUMULATE);
     */
  }

  // static Pattern pattern = Pattern.compile("(\\{.+?\\})");

  private static void addOperatorEntry(String label, Operator op) {
    if (!operatorMap.containsKey(label)) {
      operatorMap.put(label, op);
    }
    logger.info("Adding operator: " + op);
  }

  private static void addAggregatorEntry(String label, Aggregator agg) {
    if (!aggregatorMap.containsKey(label.toUpperCase())) {
      aggregatorMap.put(label.toUpperCase(), agg);
    }
    logger.info("Adding aggregator: " + agg);
  }

  /*
   * private static void addEntry(String label, ExpressionOperatorType type) {
   * if (!operatorMap.containsKey(label)) { operatorMap.put(label, type); }
   * logger.info("Adding operator type: "+type); }
   */

  public static boolean isExpressionNested(String expression) {
    return expression.contains("(");
  }

  /*
   * public static Operator getOperatorType(String expression) throws Exception
   * { String op = expression.substring(0,expression.indexOf("(")); if
   * (!operatorMap.containsKey(op)) { throw new
   * Exception(op+" is not a valid op type"); } return operatorMap.get(op); }
   */

  public static String getInnerExpression(String expression) {
    return expression.substring(expression.indexOf("(") + 1, expression.lastIndexOf(")"));
  }

  /*
   * public static String[] getBaseStats(ExpressionOperatorType type, String
   * expression) throws Exception { String[] items = null; if
   * (isExpressionNested(expression)) { ExpressionOperatorType nextType =
   * getOperatorType(expression); String innerExp =
   * getInnerExpression(expression); items = getBaseStats(nextType, innerExp); }
   * else { //base class, no nesting items = expression.split(","); }
   * if (type != null && type.isBaseOp()) { //surround items with type. for (int
   * i=0; i<items.length; i++) { items[i] = type + "(" + items[i] + ")"; //!!!!
   * NEED type to behave like string here
   * logger.debug("Forming item "+items[i]); } } return items; }
   * public static String[] getBaseStats(String expression) throws Exception {
   * expression = expression.replaceAll("\\s+", ""); return getBaseStats(null,
   * expression); }
   */

  /*
   * Validate 2 sets of parenthesis exist, all before first opDelim
   * extract agg type and validate it exists. validate number of args passed in
   */
  public static void validateAggregatorFormat(String expression) throws HelixException {
    logger.debug("validating aggregator for expression: " + expression);
    // have 0 or more args, 1 or more stats...e.g. ()(x) or (2)(x,y)
    Pattern pattern = Pattern.compile("\\(.*?\\)");
    Matcher matcher = pattern.matcher(expression);
    String aggComponent = null;
    String statComponent = null;
    int lastMatchEnd = -1;
    if (matcher.find()) {
      aggComponent = matcher.group();
      aggComponent = aggComponent.substring(1, aggComponent.length() - 1);
      if (aggComponent.contains(")") || aggComponent.contains("(")) {
        throw new HelixException(expression + " has invalid aggregate component");
      }
    } else {
      throw new HelixException(expression + " has invalid aggregate component");
    }
    if (matcher.find()) {
      statComponent = matcher.group();
      statComponent = statComponent.substring(1, statComponent.length() - 1);
      // statComponent must have at least 1 arg between paren
      if (statComponent.contains(")") || statComponent.contains("(") || statComponent.length() == 0) {
        throw new HelixException(expression + " has invalid stat component");
      }
      lastMatchEnd = matcher.end();
    } else {
      throw new HelixException(expression + " has invalid stat component");
    }
    if (matcher.find()) {
      throw new HelixException(expression + " has too many parenthesis components");
    }

    if (expression.length() >= lastMatchEnd + 1) { // lastMatchEnd is pos 1 past the pattern. check
                                                   // if there are paren there
      if (expression.substring(lastMatchEnd).contains("(")
          || expression.substring(lastMatchEnd).contains(")")) {
        throw new HelixException(expression + " has extra parenthesis");
      }
    }

    // check wildcard locations. each part can have at most 1 wildcard, and must
    // be at end
    // String expStatNamePart = expression.substring(expression.)
    StringTokenizer fieldTok = new StringTokenizer(statComponent, statFieldDelim);
    while (fieldTok.hasMoreTokens()) {
      String currTok = fieldTok.nextToken();
      if (currTok.contains(wildcardChar)) {
        if (currTok.indexOf(wildcardChar) != currTok.length() - 1
            || currTok.lastIndexOf(wildcardChar) != currTok.length() - 1) {
          throw new HelixException(currTok
              + " is illegal stat name.  Single wildcard must appear at end.");
        }
      }
    }
  }

  public static boolean statContainsWildcards(String stat) {
    return stat.contains(wildcardChar);
  }

  /*
   * Return true if stat name matches exactly...incomingStat has no agg type
   * currentStat can have any
   * Function can match for 2 cases extractStatFromAgg=false. Match
   * accumulate()(dbFoo.partition10.latency) with
   * accumulate()(dbFoo.partition10.latency)...trival extractStatFromAgg=true.
   * Match accumulate()(dbFoo.partition10.latency) with
   * dbFoo.partition10.latency
   */
  public static boolean isExactMatch(String currentStat, String incomingStat,
      boolean extractStatFromAgg) {
    String currentStatName = currentStat;
    if (extractStatFromAgg) {
      currentStatName = getSingleAggregatorStat(currentStat);
    }
    return (incomingStat.equals(currentStatName));
  }

  /*
   * Return true if incomingStat matches wildcardStat except currentStat has 1+
   * fields with "*" a*.c* matches a5.c7 a*.c* does not match a5.b6.c7
   * Function can match for 2 cases extractStatFromAgg=false. Match
   * accumulate()(dbFoo.partition*.latency) with
   * accumulate()(dbFoo.partition10.latency) extractStatFromAgg=true. Match
   * accumulate()(dbFoo.partition*.latency) with dbFoo.partition10.latency
   */
  public static boolean isWildcardMatch(String currentStat, String incomingStat,
      boolean statCompareOnly, ArrayList<String> bindings) {
    if (!statCompareOnly) { // need to check for match on agg type and stat
      String currentStatAggType = (currentStat.split("\\)"))[0];
      String incomingStatAggType = (incomingStat.split("\\)"))[0];
      if (!currentStatAggType.equals(incomingStatAggType)) {
        return false;
      }
    }
    // now just get the stats
    String currentStatName = getSingleAggregatorStat(currentStat);
    String incomingStatName = getSingleAggregatorStat(incomingStat);

    if (!currentStatName.contains(wildcardChar)) { // no wildcards in stat name
      return false;
    }

    String currentStatNamePattern = currentStatName.replace(".", "\\.");
    currentStatNamePattern = currentStatNamePattern.replace("*", ".*");
    boolean result = Pattern.matches(currentStatNamePattern, incomingStatName);
    if (result && bindings != null) {
      bindings.add(incomingStatName);
    }
    return result;
    /*
     * StringTokenizer currentStatTok = new StringTokenizer(currentStatName,
     * statFieldDelim);
     * StringTokenizer incomingStatTok = new StringTokenizer(incomingStatName,
     * statFieldDelim);
     * if (currentStatTok.countTokens() != incomingStatTok.countTokens())
     * { // stat names different numbers of fields
     * return false;
     * }
     * // for each token, if not wildcarded, must be an exact match
     * while (currentStatTok.hasMoreTokens())
     * {
     * String currTok = currentStatTok.nextToken();
     * String incomingTok = incomingStatTok.nextToken();
     * logger.debug("curTok: " + currTok);
     * logger.debug("incomingTok: " + incomingTok);
     * if (!currTok.contains(wildcardChar))
     * { // no wildcard, but have exact match
     * if (!currTok.equals(incomingTok))
     * { // not exact match
     * return false;
     * }
     * }
     * else
     * { // currTok has a wildcard
     * if (currTok.indexOf(wildcardChar) != currTok.length() - 1
     * || currTok.lastIndexOf(wildcardChar) != currTok.length() - 1)
     * {
     * throw new HelixException(currTok
     * + " is illegal stat name.  Single wildcard must appear at end.");
     * }
     * // for wildcard matching, need to escape parentheses on currTok, so
     * // regex works
     * // currTok = currTok.replace("(", "\\(");
     * // currTok = currTok.replace(")", "\\)");
     * // incomingTok = incomingTok.replace("(", "\\(");
     * // incomingTok = incomingTok.replace(")", "\\)");
     * String currTokPreWildcard = currTok.substring(0, currTok.length() - 1);
     * // TODO: if current token has a "(" in it, pattern compiling throws
     * // error
     * // Pattern pattern = Pattern.compile(currTokPreWildcard+".+"); //form
     * // pattern...wildcard part can be anything
     * // Matcher matcher = pattern.matcher(incomingTok); //see if incomingTok
     * // matches
     * if (incomingTok.indexOf(currTokPreWildcard) != 0)
     * {
     * // if (!matcher.find()) { //no match on one tok, return false
     * return false;
     * }
     * // get the binding
     * if (bindings != null)
     * {
     * // TODO: debug me!
     * String wildcardBinding = incomingTok.substring(incomingTok
     * .indexOf(currTokPreWildcard) + currTokPreWildcard.length());
     * bindings.add(wildcardBinding);
     * }
     * }
     * }
     * // all fields match or wildcard match...return true!
     * return true;
     */
  }

  /*
   * For checking if an incoming stat (no agg type defined) matches a persisted
   * stat (with agg type defined)
   */
  public static boolean isIncomingStatExactMatch(String currentStat, String incomingStat) {
    return isExactMatch(currentStat, incomingStat, true);
  }

  /*
   * For checking if an incoming stat (no agg type defined) wildcard matches a
   * persisted stat (with agg type defined) The persisted stat may have
   * wildcards
   */
  public static boolean isIncomingStatWildcardMatch(String currentStat, String incomingStat) {
    return isWildcardMatch(currentStat, incomingStat, true, null);
  }

  /*
   * For checking if a persisted stat matches a stat defined in an alert
   */
  public static boolean isAlertStatExactMatch(String alertStat, String currentStat) {
    return isExactMatch(alertStat, currentStat, false);
  }

  /*
   * For checking if a maintained stat wildcard matches a stat defined in an
   * alert. The alert may have wildcards
   */
  public static boolean isAlertStatWildcardMatch(String alertStat, String currentStat,
      ArrayList<String> wildcardBindings) {
    return isWildcardMatch(alertStat, currentStat, false, wildcardBindings);
  }

  public static Aggregator getAggregator(String aggStr) throws HelixException {
    aggStr = aggStr.toUpperCase();
    Aggregator agg = aggregatorMap.get(aggStr);
    if (agg == null) {
      throw new HelixException("Unknown aggregator type " + aggStr);
    }
    return agg;
  }

  public static String getAggregatorStr(String expression) throws HelixException {
    if (!expression.contains("(")) {
      throw new HelixException(expression
          + " does not contain a valid aggregator.  No parentheses found");
    }
    String aggName = expression.substring(0, expression.indexOf("("));
    if (!aggregatorMap.containsKey(aggName.toUpperCase())) {
      throw new HelixException("aggregator <" + aggName + "> is unknown type");
    }
    return aggName;
  }

  public static String[] getAggregatorArgs(String expression) throws HelixException {
    String aggregator = getAggregatorStr(expression);
    String argsStr = getAggregatorArgsStr(expression);
    String[] args = argsStr.split(argDelim);
    logger.debug("args size: " + args.length);
    int numArgs = (argsStr.length() == 0) ? 0 : args.length;
    // String[] argList = (expression.substring(expression.indexOf("(")+1,
    // expression.indexOf(")"))).split(argDelim);
    // verify correct number of args
    int requiredNumArgs = aggregatorMap.get(aggregator.toUpperCase()).getRequiredNumArgs();
    if (numArgs != requiredNumArgs) {
      throw new HelixException(expression + " contains " + args.length
          + " arguments, but requires " + requiredNumArgs);
    }
    return args;
  }

  /*
   * public static String[] getAggregatorArgsList(String expression) { String
   * argsStr = getAggregatorArgsStr(expression); String[] args =
   * argsStr.split(argDelim); return args; }
   */

  public static String getAggregatorArgsStr(String expression) {
    return expression.substring(expression.indexOf("(") + 1, expression.indexOf(")"));
  }

  public static String[] getAggregatorStats(String expression) throws HelixException {
    String justStats = expression;
    if (expression.contains("(") && expression.contains(")")) {
      justStats =
          (expression.substring(expression.lastIndexOf("(") + 1, expression.lastIndexOf(")")));
    }
    String[] statList = justStats.split(argDelim);
    if (statList.length < 1) {
      throw new HelixException(expression + " does not contain any aggregator stats");
    }
    return statList;
  }

  public static String getSingleAggregatorStat(String expression) throws HelixException {
    String[] stats = getAggregatorStats(expression);
    if (stats.length > 1) {
      throw new HelixException(expression + " contains more than 1 stat");
    }
    return stats[0];
  }

  public static String getWildcardStatSubstitution(String wildcardStat, String fixedStat) {
    int lastOpenParenLoc = wildcardStat.lastIndexOf("(");
    int lastCloseParenLoc = wildcardStat.lastIndexOf(")");
    StringBuilder builder = new StringBuilder();
    builder.append(wildcardStat.substring(0, lastOpenParenLoc + 1));
    builder.append(fixedStat);
    builder.append(")");
    logger.debug("wildcardStat: " + wildcardStat);
    logger.debug("fixedStat: " + fixedStat);
    logger.debug("subbedStat: " + builder.toString());
    return builder.toString();
  }

  // XXX: each op type should have number of inputs, number of outputs. do
  // validation.
  // (dbFoo.partition*.latency, dbFoo.partition*.count)|EACH|ACCUMULATE|DIVIDE
  public static String[] getBaseStats(String expression) throws HelixException {
    expression = expression.replaceAll("\\s+", "");
    validateAggregatorFormat(expression);

    String aggName = getAggregatorStr(expression);
    String[] aggArgs = getAggregatorArgs(expression);
    String[] aggStats = getAggregatorStats(expression);

    // form aggArgs
    String aggArgList = getAggregatorArgsStr(expression);

    String[] baseStats = new String[aggStats.length];
    for (int i = 0; i < aggStats.length; i++) {
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

  public static String[] getOperators(String expression) throws HelixException {
    String[] ops = null;
    int numAggStats = (getAggregatorStats(expression)).length;
    int opDelimLoc = expression.indexOf(opDelim);
    if (opDelimLoc < 0) {
      return null;
    }
    logger.debug("ops str: " + expression.substring(opDelimLoc + 1));
    ops = expression.substring(opDelimLoc + 1).split(opDelimForSplit);

    // validate this string of ops
    // verify each op exists
    // take num input tuples sets and verify ops will output exactly 1 tuple
    // sets
    int currNumTuples = numAggStats;
    for (String op : ops) {
      logger.debug("op: " + op);
      if (!operatorMap.containsKey(op.toUpperCase())) {
        throw new HelixException("<" + op + "> is not a valid operator type");
      }
      Operator currOpType = operatorMap.get(op.toUpperCase());
      if (currNumTuples < currOpType.minInputTupleLists
          || currNumTuples > currOpType.maxInputTupleLists) {
        throw new HelixException("<" + op + "> cannot process " + currNumTuples + " input tuples");
      }
      // reset num tuples to this op's output size
      if (!currOpType.inputOutputTupleListsCountsEqual) { // if equal, this number does not change
        currNumTuples = currOpType.numOutputTupleLists;
      }
    }
    if (currNumTuples != 1) {
      throw new HelixException(expression + " does not terminate in a single tuple set");
    }
    return ops;
  }

  public static void validateOperators(String expression) throws HelixException {
    getOperators(expression);
  }

  public static Operator getOperator(String opName) throws HelixException {
    if (!operatorMap.containsKey(opName)) {
      throw new HelixException(opName + " is unknown op type");
    }
    return operatorMap.get(opName);
  }

  public static void validateExpression(String expression) throws HelixException {
    // 1. extract stats part and validate
    validateAggregatorFormat(expression);
    // 2. extract ops part and validate the ops exist and the inputs/outputs are
    // correct
    validateOperators(expression);
  }
}
