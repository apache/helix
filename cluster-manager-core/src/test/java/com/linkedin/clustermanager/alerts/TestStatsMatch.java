package com.linkedin.clustermanager.alerts;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterManagerException;

@Test
public class TestStatsMatch {

	@Test
	  public void testExactMatch()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isExactMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testSingleWildcardMatch()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testDoubleWildcardMatch()
	  {
	    
	    String persistedStatName = "window(5)(db*.partition*.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchNoWildcard()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchTooManyFields()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.tableBar.partition10.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchTooFewFields()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName));
	  }
	

	@Test
	  public void testBadWildcardRepeated()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition**.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    boolean caughtException = false;
	    try {
	    	boolean match = ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName);
	    } catch (ClusterManagerException e) {
	    	caughtException = true;
	    }
	    AssertJUnit.assertTrue(caughtException);
	  }
	
	@Test
	  public void testBadWildcardNotAtEnd()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.*partition.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    boolean caughtException = false;
	    try {
	    	boolean match = ExpressionParser.isWildcardMatch(persistedStatName, incomingStatName);
	    } catch (ClusterManagerException e) {
	    	caughtException = true;
	    }
	    AssertJUnit.assertTrue(caughtException);
	  }
}
