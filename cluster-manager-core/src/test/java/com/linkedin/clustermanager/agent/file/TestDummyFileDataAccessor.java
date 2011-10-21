package com.linkedin.clustermanager.agent.file;


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
    Assert.assertFalse(accessor.setProperty(PropertyType.CONFIGS, record));
    Assert.assertFalse(accessor.updateProperty(PropertyType.CONFIGS, record));
    Assert.assertNull(accessor.getProperty(PropertyType.CONFIGS, "key1"));
    Assert.assertFalse(accessor.removeProperty(PropertyType.CONFIGS, "key1"));
    Assert.assertNull(accessor.getChildNames(PropertyType.CONFIGS, "key1"));
    Assert.assertNull(accessor.getChildValues(PropertyType.CONFIGS, "key1"));
    Assert.assertNull(accessor.getStore());
  }

}
