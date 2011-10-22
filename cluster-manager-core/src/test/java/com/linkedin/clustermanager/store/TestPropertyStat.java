package com.linkedin.clustermanager.store;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestPropertyStat
{
  @Test (groups = {"unitTest"})
  public void testPropertyStat()
  {
    PropertyStat stat = new PropertyStat(0, 0);
    AssertJUnit.assertEquals(0, stat.getLastModifiedTime());
    AssertJUnit.assertEquals(0, stat.getVersion());
  }

}
