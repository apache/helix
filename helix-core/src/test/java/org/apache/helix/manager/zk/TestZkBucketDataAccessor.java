package org.apache.helix.manager.zk;

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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkBucketDataAccessor extends ZkTestBase {

  private String PATH = "/" + TestHelper.getTestClassName();
  private String NAME_KEY = TestHelper.getTestClassName();
  private List<String> LIST_FIELD;
  private Map<String, String> MAP_FIELD;

  private BucketDataAccessor _bucketDataAccessor;

  @BeforeClass
  public void beforeClass() {
    _bucketDataAccessor = new ZkBucketDataAccessor(ZK_ADDR);

    // Populate LIST_FIELD for R/W comparison
    LIST_FIELD = new ArrayList<>();
    LIST_FIELD.add("1");
    LIST_FIELD.add("2");

    // Populate MAP_FIELD for R/W comparison
    MAP_FIELD = new HashMap<>();
    MAP_FIELD.put("1", "2");
  }

  /**
   * Attempt writing a simple HelixProperty using compressedBucketWrite.
   * @throws IOException
   */
  @Test
  public void testCompressedBucketWrite() throws IOException {
    ZNRecord record = new ZNRecord(NAME_KEY);
    record.setSimpleField(NAME_KEY, NAME_KEY);
    record.setListField(NAME_KEY, LIST_FIELD);
    record.setMapField(NAME_KEY, MAP_FIELD);
    Assert.assertTrue(_bucketDataAccessor.compressedBucketWrite(PATH, new HelixProperty(record)));
  }

  /**
   * The record written in {@link #testCompressedBucketWrite()} is the same record that was written.
   */
  @Test(dependsOnMethods = "testCompressedBucketWrite")
  public void testCompressedBucketRead() {
    HelixProperty readRecord = _bucketDataAccessor.compressedBucketRead(PATH, HelixProperty.class);
    Assert.assertEquals(readRecord.getRecord().getSimpleField(NAME_KEY), NAME_KEY);
    Assert.assertEquals(readRecord.getRecord().getListField(NAME_KEY), LIST_FIELD);
    Assert.assertEquals(readRecord.getRecord().getMapField(NAME_KEY), MAP_FIELD);
  }

  /**
   * Write a HelixProperty with large number of entries using BucketDataAccessor and read it back.
   */
  @Test(dependsOnMethods = "testCompressedBucketRead")
  public void testLargeWriteAndRead() throws IOException {
    String name = "largeResourceAssignment";
    HelixProperty property = new HelixProperty(name);

    int numEntries = 100000;
    for (int i = 0; i < numEntries; i++) {
      // Create a random string every time
      byte[] arrayKey = new byte[20];
      byte[] arrayVal = new byte[20];
      new Random().nextBytes(arrayKey);
      new Random().nextBytes(arrayVal);
      String randomStrKey = new String(arrayKey, StandardCharsets.UTF_8);
      String randomStrVal = new String(arrayVal, StandardCharsets.UTF_8);

      // Dummy mapField
      Map<String, String> mapField = new HashMap<>();
      mapField.put(randomStrKey, randomStrVal);

      property.getRecord().setMapField(randomStrKey, mapField);
    }

    // Perform large write
    long before = System.currentTimeMillis();
    _bucketDataAccessor.compressedBucketWrite("/" + name, property);
    long after = System.currentTimeMillis();
    System.out.println("Write took " + (after - before) + " ms");

    // Read it back
    before = System.currentTimeMillis();
    HelixProperty readRecord =
        _bucketDataAccessor.compressedBucketRead("/" + name, HelixProperty.class);
    after = System.currentTimeMillis();
    System.out.println("Read took " + (after - before) + " ms");

    // Check against the original HelixProperty
    Assert.assertEquals(readRecord, property);
  }
}
