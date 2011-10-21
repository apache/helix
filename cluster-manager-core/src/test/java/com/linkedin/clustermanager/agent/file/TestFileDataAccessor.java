package com.linkedin.clustermanager.agent.file;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class TestFileDataAccessor
{
  @Test(groups = { "unitTest" })
  public void testFileDataAccessor()
  {
    final String clusterName = "TestFileDataAccessor";
    String rootNamespace = "/tmp/" + clusterName;
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    FilePropertyStore<ZNRecord> store = new FilePropertyStore<ZNRecord>(serializer, rootNamespace, comparator);
    try
    {
      store.removeRootNamespace();
    }
    catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ClusterDataAccessor accessor = new FileBasedDataAccessor(store, clusterName);
    
    ZNRecord record = new ZNRecord("id0");
    accessor.setProperty(PropertyType.CONFIGS, record, "key0");
    record = accessor.getProperty(PropertyType.CONFIGS, "key0");
    Assert.assertEquals("id0", record.getId());
    
    ZNRecord newRecord = new ZNRecord("id1");
    accessor.updateProperty(PropertyType.CONFIGS, newRecord,"key0");
    record = accessor.getProperty(PropertyType.CONFIGS, "key0");
    Assert.assertEquals("id1", record.getId());
    
    accessor.removeProperty(PropertyType.CONFIGS, "key0");
    record = accessor.getProperty(PropertyType.CONFIGS, "key0");
    Assert.assertNull(record);
    Assert.assertNull(accessor.getChildNames(PropertyType.CONFIGS, "key0"));
    
    
    store.stop();
  }

}
