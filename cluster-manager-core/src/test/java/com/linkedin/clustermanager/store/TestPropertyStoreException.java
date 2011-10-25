package com.linkedin.clustermanager.store;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestPropertyStoreException
{
  @Test (groups = {"unitTest"})
  public void testPropertyStoreException()
  {
    PropertyStoreException exception = new PropertyStoreException("msg");
    AssertJUnit.assertEquals(exception.getMessage(), "msg");
    
    exception = new PropertyStoreException();
    AssertJUnit.assertNull(exception.getMessage());
  }

}
