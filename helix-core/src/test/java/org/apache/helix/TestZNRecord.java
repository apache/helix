package org.apache.helix;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestZNRecord {

  @Test
  public void testEquals() {
    ZNRecord record1 = new ZNRecord("id");
    record1.setSimpleField("k1", "v1");
    record1.setMapField("m1", new HashMap<String, String>());
    record1.getMapField("m1").put("k1", "v1");
    record1.setListField("l1", new ArrayList<String>());
    record1.getListField("l1").add("v1");
    ZNRecord record2 = new ZNRecord("id");
    record2.setSimpleField("k1", "v1");
    record2.setMapField("m1", new HashMap<String, String>());
    record2.getMapField("m1").put("k1", "v1");
    record2.setListField("l1", new ArrayList<String>());
    record2.getListField("l1").add("v1");

    AssertJUnit.assertEquals(record1, record2);
    record2.setSimpleField("k2", "v1");
    AssertJUnit.assertNotSame(record1, record2);
  }

  @Test
  public void testMerge() {
    ZNRecord record = new ZNRecord("record1");

    // set simple field
    record.setSimpleField("simpleKey1", "simpleValue1");

    // set list field
    List<String> list1 = new ArrayList<String>();
    list1.add("list1Value1");
    list1.add("list1Value2");
    record.setListField("listKey1", list1);

    // set map field
    Map<String, String> map1 = new HashMap<String, String>();
    map1.put("map1Key1", "map1Value1");
    record.setMapField("mapKey1", map1);
    // System.out.println(record);

    ZNRecord updateRecord = new ZNRecord("updateRecord");

    // set simple field
    updateRecord.setSimpleField("simpleKey2", "simpleValue2");

    // set list field
    List<String> newList1 = new ArrayList<String>();
    newList1.add("list1Value1");
    newList1.add("list1Value2");
    newList1.add("list1NewValue1");
    newList1.add("list1NewValue2");
    updateRecord.setListField("listKey1", newList1);

    List<String> list2 = new ArrayList<String>();
    list2.add("list2Value1");
    list2.add("list2Value2");
    updateRecord.setListField("listKey2", list2);

    // set map field
    Map<String, String> newMap1 = new HashMap<String, String>();
    newMap1.put("map1NewKey1", "map1NewValue1");
    updateRecord.setMapField("mapKey1", newMap1);

    Map<String, String> map2 = new HashMap<String, String>();
    map2.put("map2Key1", "map2Value1");
    updateRecord.setMapField("mapKey2", map2);
    // System.out.println(updateRecord);

    record.merge(updateRecord);
    // System.out.println(record);

    ZNRecord expectRecord = new ZNRecord("record1");
    expectRecord.setSimpleField("simpleKey1", "simpleValue1");
    expectRecord.setSimpleField("simpleKey2", "simpleValue2");
    List<String> expectList1 = new ArrayList<String>();
    expectList1.add("list1Value1");
    expectList1.add("list1Value2");
    expectList1.add("list1Value1");
    expectList1.add("list1Value2");
    expectList1.add("list1NewValue1");
    expectList1.add("list1NewValue2");
    expectRecord.setListField("listKey1", expectList1);
    List<String> expectList2 = new ArrayList<String>();
    expectList2.add("list2Value1");
    expectList2.add("list2Value2");
    expectRecord.setListField("listKey2", expectList2);
    Map<String, String> expectMap1 = new HashMap<String, String>();
    expectMap1.put("map1Key1", "map1Value1");
    expectMap1.put("map1NewKey1", "map1NewValue1");
    expectRecord.setMapField("mapKey1", expectMap1);
    Map<String, String> expectMap2 = new HashMap<String, String>();
    expectMap2.put("map2Key1", "map2Value1");
    expectRecord.setMapField("mapKey2", expectMap2);
    Assert.assertEquals(record, expectRecord, "Should be equal.");
  }

  @Test
  public void testSubtract() {
    ZNRecord record = new ZNRecord("test");
    Map<String, String> map = new HashMap<String, String>();
    map.put("mapKey1", "mapValue1");
    map.put("mapKey2", "mapValue2");
    record.setMapField("key1", map);

    ZNRecord delta = new ZNRecord("test");
    Map<String, String> deltaMap = new HashMap<String, String>();
    deltaMap.put("mapKey1", "mapValue1");
    delta.setMapField("key1", deltaMap);

    record.subtract(delta);

    Assert.assertEquals(record.getMapFields().size(), 1);
    Assert.assertNotNull(record.getMapField("key1"));
    Assert.assertEquals(record.getMapField("key1").size(), 1);
    Assert.assertNotNull(record.getMapField("key1").get("mapKey2"));
    Assert.assertEquals(record.getMapField("key1").get("mapKey2"), "mapValue2");
  }
}
