/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.helix.manager.file;

import org.apache.helix.DataAccessor;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.ConfigScope.ConfigScopeProperty;
import org.apache.helix.manager.file.FileDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.store.file.FilePropertyStore;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestFileDataAccessor
{
  @Test()
  public void testFileDataAccessor()
  {
    final String clusterName = "TestFileDataAccessor";
    String rootNamespace = "/tmp/" + clusterName;
    PropertyJsonSerializer<ZNRecord> serializer = new PropertyJsonSerializer<ZNRecord>(
        ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator = new PropertyJsonComparator<ZNRecord>(
        ZNRecord.class);
    FilePropertyStore<ZNRecord> store = new FilePropertyStore<ZNRecord>(serializer, rootNamespace,
        comparator);
    try
    {
      store.removeRootNamespace();
    } catch (PropertyStoreException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    DataAccessor accessor = new FileDataAccessor(store, clusterName);

    InstanceConfig config = new InstanceConfig("id0");
    accessor.setProperty(PropertyType.CONFIGS, config, ConfigScopeProperty.PARTICIPANT.toString(),
        "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS,
        ConfigScopeProperty.PARTICIPANT.toString(), "key0");
    AssertJUnit.assertEquals("id0", config.getId());

    InstanceConfig newConfig = new InstanceConfig("id1");
    accessor.updateProperty(PropertyType.CONFIGS, newConfig,
        ConfigScopeProperty.PARTICIPANT.toString(),
        "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS,
        ConfigScopeProperty.PARTICIPANT.toString(), "key0");
    AssertJUnit.assertEquals("id1", config.getId());

    accessor.removeProperty(PropertyType.CONFIGS, ConfigScopeProperty.PARTICIPANT.toString(),
        "key0");
    config = accessor.getProperty(InstanceConfig.class, PropertyType.CONFIGS,
        ConfigScopeProperty.PARTICIPANT.toString(), "key0");
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

    // List<ZNRecord> childs = accessor.getChildValues(PropertyType.HISTORY);
    // AssertJUnit.assertEquals(childs.size(), 0);

    store.stop();
  }

}
