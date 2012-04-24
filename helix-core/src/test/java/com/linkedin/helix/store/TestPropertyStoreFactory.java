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
