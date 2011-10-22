package com.linkedin.clustermanager.store;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.ZkUnitTestBase;

public class TestPropertyStoreFactory extends ZkUnitTestBase
{
  @Test (groups = {"unitTest"})
  public void testZkPropertyStoreFactory()
  {
    final String rootNamespace = "TestPropertyStoreFactory";
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyStore<ZNRecord> zkStore;
    
    boolean exceptionCaught = false;
    try
    {
      zkStore = PropertyStoreFactory.<ZNRecord>getZKPropertyStore(null, null, null);
    } catch (IllegalArgumentException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    zkStore = PropertyStoreFactory.<ZNRecord>getZKPropertyStore(ZK_ADDR, serializer, rootNamespace);
  }

  @Test (groups = {"unitTest"})
  public void testFilePropertyStoreFactory()
  {
    final String rootNamespace = "/tmp/TestPropertyStoreFactory";
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    PropertyStore<ZNRecord> fileStore;
    
    boolean exceptionCaught = false;
    try
    {
      fileStore = PropertyStoreFactory.<ZNRecord>getFilePropertyStore(null, null, null);
    } catch (IllegalArgumentException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);
    
    fileStore = PropertyStoreFactory.<ZNRecord>getFilePropertyStore(serializer, rootNamespace, comparator);
  }

}
