package com.linkedin.clustermanager.alerts;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ClusterManagerException;

public class AlertParser {
	private static Logger logger = Logger.getLogger(AlertParser.class);
	
	public static final String EXPRESSION_NAME = "EXP";
	public static final String COMPARATOR_NAME = "CMP";
	public static final String CONSTANT_NAME = "CON";
	
	static Map<String, AlertComparator> comparatorMap = new HashMap<String, AlertComparator>();
	
	static
	  {
		  
		  addComparatorEntry("GREATER", new GreaterAlertComparator());
	  }
	
	 private static void addComparatorEntry(String label, AlertComparator comp)
	  {
		  if (!comparatorMap.containsKey(label))
		    {
		      comparatorMap.put(label, comp);
		    }
		    logger.info("Adding comparator: "+comp);
	  }
	
	public static AlertComparator getComparator(String compName)
	{
		compName = compName.replaceAll("\\s+", ""); //remove white space
		if (!comparatorMap.containsKey(compName)) {
			throw new ClusterManagerException("Comparator type <"+compName+"> unknown");
		}
		return comparatorMap.get(compName);
	}
	 
	public static String getComponent(String component, String alert) throws ClusterManagerException
	{
		//find EXP and keep going until paren are closed
		int expStartPos = alert.indexOf(component);
		if (expStartPos < 0) {
			throw new ClusterManagerException(alert+" does not contain component "+component);
		}
		expStartPos += (component.length()+1); //advance length of string and one for open paren
		int expEndPos = expStartPos;
		int openParenCount = 1;
		while (openParenCount > 0) {
			if (alert.charAt(expEndPos) == '(') {
				openParenCount++;
			}
			else if (alert.charAt(expEndPos) == ')') {
				openParenCount--;
			}
			expEndPos++;
		}
		if (openParenCount != 0) {
			throw new ClusterManagerException(alert+" does not contain valid "+component+" component, " +
					"parentheses do not close");
		}
		//return what is in between paren
		return alert.substring(expStartPos, expEndPos-1);
	}
	
	public static boolean validateAlert(String alert) throws ClusterManagerException
	{
		//TODO: decide if toUpperCase is going to cause problems with stuff like db name
		alert = alert.replaceAll("\\s+", ""); //remove white space
		String exp = getComponent(EXPRESSION_NAME, alert);
		String cmp = getComponent(COMPARATOR_NAME, alert);
		String val = getComponent(CONSTANT_NAME, alert);
		logger.debug("exp: "+exp);
		logger.debug("cmp: "+cmp);
		logger.debug("val: "+val);
		
		//separately validate each portion
		ExpressionParser.validateExpression(exp);
		
		//validate comparator
		if (!comparatorMap.containsKey(cmp.toUpperCase())) {
			throw new ClusterManagerException("Unknown comparator type "+cmp);
		}
		
		//ValParser.  Probably don't need this.  Just make sure it's a valid tuple.  But would also be good
		//to validate that the tuple is same length as exp's output...maybe leave that as future todo
		//not sure we can really do much here though...anything can be in a tuple.
		
		//TODO: try to compare tuple width of CON against tuple width of agg type!  Not a good idea, what if
		//is not at full width yet, like with window  
		
		//if all of this passes, then we can safely record the alert in zk.  still need to implement zk location
		
		return false;
	}
}
