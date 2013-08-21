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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.DefaultParticipantErrorMessageHandlerFactory.ActionOnError;
import org.apache.log4j.Logger;

public class AlertParser {
  private static Logger logger = Logger.getLogger(AlertParser.class);

  public static final String EXPRESSION_NAME = "EXP";
  public static final String COMPARATOR_NAME = "CMP";
  public static final String CONSTANT_NAME = "CON";
  public static final String ACTION_NAME = "ACTION";

  static Map<String, AlertComparator> comparatorMap = new HashMap<String, AlertComparator>();

  static {

    addComparatorEntry("GREATER", new GreaterAlertComparator());
  }

  private static void addComparatorEntry(String label, AlertComparator comp) {
    if (!comparatorMap.containsKey(label)) {
      comparatorMap.put(label, comp);
    }
    logger.info("Adding comparator: " + comp);
  }

  public static AlertComparator getComparator(String compName) {
    compName = compName.replaceAll("\\s+", ""); // remove white space
    if (!comparatorMap.containsKey(compName)) {
      throw new HelixException("Comparator type <" + compName + "> unknown");
    }
    return comparatorMap.get(compName);
  }

  public static String getComponent(String component, String alert) throws HelixException {
    // find EXP and keep going until paren are closed
    int expStartPos = alert.indexOf(component);
    if (expStartPos < 0) {
      throw new HelixException(alert + " does not contain component " + component);
    }
    expStartPos += (component.length() + 1); // advance length of string and one for open paren
    int expEndPos = expStartPos;
    int openParenCount = 1;
    while (openParenCount > 0) {
      if (alert.charAt(expEndPos) == '(') {
        openParenCount++;
      } else if (alert.charAt(expEndPos) == ')') {
        openParenCount--;
      }
      expEndPos++;
    }
    if (openParenCount != 0) {
      throw new HelixException(alert + " does not contain valid " + component + " component, "
          + "parentheses do not close");
    }
    // return what is in between paren
    return alert.substring(expStartPos, expEndPos - 1);
  }

  public static boolean validateAlert(String alert) throws HelixException {
    // TODO: decide if toUpperCase is going to cause problems with stuff like db name
    alert = alert.replaceAll("\\s+", ""); // remove white space
    String exp = getComponent(EXPRESSION_NAME, alert);
    String cmp = getComponent(COMPARATOR_NAME, alert);
    String val = getComponent(CONSTANT_NAME, alert);
    logger.debug("exp: " + exp);
    logger.debug("cmp: " + cmp);
    logger.debug("val: " + val);

    // separately validate each portion
    ExpressionParser.validateExpression(exp);

    // validate comparator
    if (!comparatorMap.containsKey(cmp.toUpperCase())) {
      throw new HelixException("Unknown comparator type " + cmp);
    }
    String actionValue = null;
    try {
      actionValue = AlertParser.getComponent(AlertParser.ACTION_NAME, alert);
    } catch (Exception e) {
      logger.info("No action specified in " + alert);
    }

    if (actionValue != null) {
      validateActionValue(actionValue);
    }
    // ValParser. Probably don't need this. Just make sure it's a valid tuple. But would also be
    // good
    // to validate that the tuple is same length as exp's output...maybe leave that as future todo
    // not sure we can really do much here though...anything can be in a tuple.

    // TODO: try to compare tuple width of CON against tuple width of agg type! Not a good idea,
    // what if
    // is not at full width yet, like with window

    // if all of this passes, then we can safely record the alert in zk. still need to implement zk
    // location

    return false;
  }

  public static void validateActionValue(String actionValue) {
    try {
      ActionOnError actionVal = ActionOnError.valueOf(actionValue);
    } catch (Exception e) {
      String validActions = "";
      for (ActionOnError action : ActionOnError.values()) {
        validActions = validActions + action + " ";
      }
      throw new HelixException("Unknown cmd type " + actionValue + ", valid types : "
          + validActions);
    }
  }
}
