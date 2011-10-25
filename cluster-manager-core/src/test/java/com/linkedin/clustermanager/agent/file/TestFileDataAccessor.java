package com.linkedin.clustermanager.agent.file;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import java.util.List;

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
    AssertJUnit.assertEquals("id0", record.getId());
    
    ZNRecord newRecord = new ZNRecord("id1");
    accessor.updateProperty(PropertyType.CONFIGS, newRecord,"key0");
    record = accessor.getProperty(PropertyType.CONFIGS, "key0");
    AssertJUnit.assertEquals("id1", record.getId());
    
    accessor.removeProperty(PropertyType.CONFIGS, "key0");
    record = accessor.getProperty(PropertyType.CONFIGS, "key0");
    AssertJUnit.assertNull(record);
    AssertJUnit.assertNull(accessor.getChildNames(PropertyType.CONFIGS, "key0"));
    
    ZNRecord leaderRecord = new ZNRecord("id2");
    accessor.updateProperty(PropertyType.LEADER, leaderRecord);
    record = accessor.getProperty(PropertyType.LEADER);
    AssertJUnit.assertNull(record);
    
    accessor.setProperty(PropertyType.LEADER, leaderRecord);
    ZNRecord newLeaderRecord = new ZNRecord("id3");
    accessor.updateProperty(PropertyType.LEADER, newLeaderRecord);
    record = accessor.getProperty(PropertyType.LEADER);
    AssertJUnit.assertEquals("id3", newLeaderRecord.getId());
    
    List<ZNRecord> childs = accessor.getChildValues(PropertyType.HISTORY);
    AssertJUnit.assertEquals(childs.size(), 0);
    
    store.stop();
  }

}
