package com.linkedin.clustermanager.store;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ZNRecord;

public class TestJsonComparator
{
  @Test (groups = {"unitTest"})
  public void testJsonComparator()
  {
    ZNRecord record = new ZNRecord("id1");
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    AssertJUnit.assertTrue(comparator.compare(null, null) == 0);
    AssertJUnit.assertTrue(comparator.compare(null, record) == -1);
    AssertJUnit.assertTrue(comparator.compare(record, null) == 1);
  }
}
