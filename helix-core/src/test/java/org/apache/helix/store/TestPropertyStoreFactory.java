package org.apache.helix.store;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.store.PropertyJsonComparator;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStore;
import org.apache.helix.store.PropertyStoreFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestPropertyStoreFactory extends ZkUnitTestBase
{
  @Test()
  public void testZkPropertyStoreFactory()
  {

    try
    {
      PropertyStoreFactory.<ZNRecord> getZKPropertyStore(null, null, null);
      Assert.fail("Should fail since zkAddr|serializer|root can't be null");
    }
    catch (IllegalArgumentException e)
    {
      // OK
    }
    
    final String rootNamespace = "TestPropertyStoreFactory";
    PropertyJsonSerializer<ZNRecord> serializer =
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    try
    {
      PropertyStoreFactory.<ZNRecord> getZKPropertyStore("localhost:1812", serializer, rootNamespace);
      Assert.fail("Should fail since zkAddr is not connectable");
    }
    catch (Exception e)
    {
      // OK
    }

    PropertyStore<ZNRecord> store =
        PropertyStoreFactory.<ZNRecord> getZKPropertyStore(ZK_ADDR,
                                                           serializer,
                                                           rootNamespace);
    Assert.assertNotNull(store);
  }

  @Test()
  public void testFilePropertyStoreFactory()
  {
    final String rootNamespace = "/tmp/TestPropertyStoreFactory";
    PropertyJsonSerializer<ZNRecord> serializer =
        new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);
    PropertyJsonComparator<ZNRecord> comparator =
        new PropertyJsonComparator<ZNRecord>(ZNRecord.class);
    PropertyStore<ZNRecord> store;

    boolean exceptionCaught = false;
    try
    {
      store = PropertyStoreFactory.<ZNRecord> getFilePropertyStore(null, null, null);
    }
    catch (IllegalArgumentException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    store =
        PropertyStoreFactory.<ZNRecord> getFilePropertyStore(serializer,
                                                             rootNamespace,
                                                             comparator);
    Assert.assertNotNull(store);
  }

}
