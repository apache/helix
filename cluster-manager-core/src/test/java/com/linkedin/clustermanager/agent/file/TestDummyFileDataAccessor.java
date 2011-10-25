package com.linkedin.clustermanager.agent.file;


import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;

public class TestDummyFileDataAccessor
{
  @Test(groups = { "unitTest" })
  public void testDummyFileDataAccessor()
  {
    DummyFileDataAccessor accessor = new DummyFileDataAccessor();
    ZNRecord record = new ZNRecord("id1");
    AssertJUnit.assertFalse(accessor.setProperty(PropertyType.CONFIGS, record));
    AssertJUnit.assertFalse(accessor.updateProperty(PropertyType.CONFIGS, record));
    AssertJUnit.assertNull(accessor.getProperty(PropertyType.CONFIGS, "key1"));
    AssertJUnit.assertFalse(accessor.removeProperty(PropertyType.CONFIGS, "key1"));
    AssertJUnit.assertNull(accessor.getChildNames(PropertyType.CONFIGS, "key1"));
    AssertJUnit.assertNull(accessor.getChildValues(PropertyType.CONFIGS, "key1"));
    AssertJUnit.assertNull(accessor.getStore());
  }

}
