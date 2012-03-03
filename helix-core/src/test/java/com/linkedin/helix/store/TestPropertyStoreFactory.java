package com.linkedin.helix.store;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;

public class TestPropertyStoreFactory extends ZkUnitTestBase
{
  @Test ()
  public void testZkPropertyStoreFactory()
  {
    final String rootNamespace = "TestPropertyStoreFactory";
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    try
    {
      PropertyStoreFactory.<ZNRecord>getZKPropertyStore(null, null, null);
      Assert.fail("Should fail since zkAddr|serializer|root can't be null");
    } catch (IllegalArgumentException e)
    {
      // OK
    }
    
    PropertyStoreFactory.<ZNRecord>getZKPropertyStore(ZK_ADDR, serializer, rootNamespace);
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
