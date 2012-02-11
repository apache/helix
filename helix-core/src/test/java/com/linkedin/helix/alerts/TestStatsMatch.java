package com.linkedin.helix.alerts;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.alerts.ExpressionParser;

@Test
public class TestStatsMatch {

	@Test
	  public void testExactMatch()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatExactMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testSingleWildcardMatch()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testDoubleWildcardMatch()
	  {
	    
	    String persistedStatName = "window(5)(db*.partition*.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertTrue(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchNoWildcard()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition10.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchTooManyFields()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.tableBar.partition10.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName));
	  }
	
	@Test
	  public void testWildcardMatchTooFewFields()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition*.latency)";
	    String incomingStatName = "dbFoo.latency";
	    AssertJUnit.assertFalse(ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName));
	  }
	

	@Test
	  public void testBadWildcardRepeated()
	  {
	    
	    String persistedStatName = "window(5)(dbFoo.partition**.latency)";
	    String incomingStatName = "dbFoo.partition10.latency";
	    boolean caughtException = false;
	    try {
	    	boolean match = ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName);
	    } catch (HelixException e) {
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
	    	boolean match = ExpressionParser.isIncomingStatWildcardMatch(persistedStatName, incomingStatName);
	    } catch (HelixException e) {
	    	caughtException = true;
	    }
	    AssertJUnit.assertTrue(caughtException);
	  }
}
