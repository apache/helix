package org.apache.helix.manager.zk;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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

public class TestZNRecordStreamingSerializer {
  /**
   * Test the normal case of serialize/deserialize where ZNRecord is well-formed
   */
  @Test
  public void basicTest() {
    ZNRecord record = new ZNRecord("testId");
    record.setMapField("k1", ImmutableMap.of("a", "b", "c", "d"));
    record.setMapField("k2", ImmutableMap.of("e", "f", "g", "h"));
    record.setListField("k3", ImmutableList.of("a", "b", "c", "d"));
    record.setListField("k4", ImmutableList.of("d", "e", "f", "g"));
    record.setSimpleField("k5", "a");
    record.setSimpleField("k5", "b");
    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(serializer.serialize(record));
    Assert.assertEquals(result, record);
  }

  /**
   * Check that the ZNRecord is not constructed if there is no id in the json
   */
  @Test
  public void noIdTest() {
    StringBuilder jsonString =
        new StringBuilder("{\n").append("  \"simpleFields\": {},\n")
            .append("  \"listFields\": {},\n").append("  \"mapFields\": {}\n").append("}\n");
    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(jsonString.toString().getBytes());
    Assert.assertNull(result);
  }

  /**
   * Test that the json still deserizalizes correctly if id is not first
   */
  @Test
  public void idNotFirstTest() {
    StringBuilder jsonString =
        new StringBuilder("{\n").append("  \"simpleFields\": {},\n")
            .append("  \"listFields\": {},\n").append("  \"mapFields\": {},\n")
            .append("\"id\": \"myId\"\n").append("}");
    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(jsonString.toString().getBytes());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getId(), "myId");
  }

  /**
   * Test that simple, list, and map fields are initialized as empty even when not in json
   */
  @Test
  public void fieldAutoInitTest() {
    StringBuilder jsonString = new StringBuilder("{\n").append("\"id\": \"myId\"\n").append("}");
    ZNRecordStreamingSerializer serializer = new ZNRecordStreamingSerializer();
    ZNRecord result = (ZNRecord) serializer.deserialize(jsonString.toString().getBytes());
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getId(), "myId");
    Assert.assertNotNull(result.getSimpleFields());
    Assert.assertTrue(result.getSimpleFields().isEmpty());
    Assert.assertNotNull(result.getListFields());
    Assert.assertTrue(result.getListFields().isEmpty());
    Assert.assertNotNull(result.getMapFields());
    Assert.assertTrue(result.getMapFields().isEmpty());
  }
}
