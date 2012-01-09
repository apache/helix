package com.linkedin.clustermanager.agent.file;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.InstanceConfig;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.store.PropertyJsonComparator;
import com.linkedin.clustermanager.store.PropertyJsonSerializer;
import com.linkedin.clustermanager.store.PropertyStoreException;
import com.linkedin.clustermanager.store.file.FilePropertyStore;

public class TestFileDataAccessor
{
  @Test()
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

    InstanceConfig config = new InstanceConfig("id0");
    accessor.setProperty(PropertyType.CONFIGS, config, "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS, "key0");
    AssertJUnit.assertEquals("id0", config.getId());

    InstanceConfig newConfig = new InstanceConfig("id1");
    accessor.updateProperty(PropertyType.CONFIGS, newConfig, "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS, "key0");
    AssertJUnit.assertEquals("id1", config.getId());

    accessor.removeProperty(PropertyType.CONFIGS, "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS, "key0");
    AssertJUnit.assertNull(config);

    LiveInstance leader = new LiveInstance("id2");
    accessor.updateProperty(PropertyType.LEADER, leader);
    LiveInstance nullLeader = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertNull(nullLeader);

    accessor.setProperty(PropertyType.LEADER, leader);
    LiveInstance newLeader = new LiveInstance("id3");
    accessor.updateProperty(PropertyType.LEADER, newLeader);
    leader = accessor.getProperty(LiveInstance.class, PropertyType.LEADER);
    AssertJUnit.assertEquals("id3", leader.getId());

//    List<ZNRecord> childs = accessor.getChildValues(PropertyType.HISTORY);
//    AssertJUnit.assertEquals(childs.size(), 0);

    store.stop();
  }

}
