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

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.zk.ZKPropertyStore;

public class TestZNRecordJsonSerializer extends ZkUnitTestBase
{

  @Test
  public void testZNRecordJsonSerializer() throws Exception
  {
    final String testRoot = getShortClassName();

    System.out.println("START " + testRoot + " at " + new Date(System.currentTimeMillis()));

    ZNRecord record = new ZNRecord("node1");
    record.setSimpleField(ZNRecord.LIST_FIELD_BOUND, "" + 3);
    List<String> list1 = Arrays.asList("one", "two", "three", "four");
    List<String> list2 = Arrays.asList("a", "b", "c", "d");
    List<String> list3 = Arrays.asList("x", "y");
    record.setListField("list1", list1);
    record.setListField("list2", list2);
    record.setListField("list3", list3);

    ZKPropertyStore<ZNRecord> store = new ZKPropertyStore<ZNRecord>(new ZkClient(ZK_ADDR),
        new ZNRecordJsonSerializer(), "/" + testRoot);

    store.setProperty("node1", record);
    ZNRecord newRecord = store.getProperty("node1");
    list1 = newRecord.getListField("list1");
    Assert.assertTrue(list1.size() == 3);
    Assert.assertTrue(list1.contains("one"));
    Assert.assertTrue(list1.contains("two"));
    Assert.assertTrue(list1.contains("three"));

    list2 = newRecord.getListField("list2");
    Assert.assertTrue(list2.size() == 3);
    Assert.assertTrue(list2.contains("a"));
    Assert.assertTrue(list2.contains("b"));
    Assert.assertTrue(list2.contains("c"));

    list3 = newRecord.getListField("list3");
    Assert.assertTrue(list3.size() == 2);
    Assert.assertTrue(list3.contains("x"));
    Assert.assertTrue(list3.contains("y"));

    System.out.println("END " + testRoot + " at " + new Date(System.currentTimeMillis()));

  }
}

