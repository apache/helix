package com.linkedin.clustermanager.store;

import java.util.Date;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ZNRecord;

public class TestJsonComparator
{
  @Test (groups = {"unitTest"})
  public void testJsonComparator()
  {
    System.out.println("START TestJsonComparator at " + new Date(System.currentTimeMillis()));

    ZNRecord record = new ZNRecord("id1");
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    AssertJUnit.assertTrue(comparator.compare(null, null) == 0);
    AssertJUnit.assertTrue(comparator.compare(null, record) == -1);
    AssertJUnit.assertTrue(comparator.compare(record, null) == 1);
    System.out.println("END TestJsonComparator at " + new Date(System.currentTimeMillis()));
  }
}
