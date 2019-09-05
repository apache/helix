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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.helix.AccessOption;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkBucketDataAccessor extends ZkTestBase {

  private static final String PATH = "/" + TestHelper.getTestClassName();
  private static final String NAME_KEY = TestHelper.getTestClassName();
  private static final String LAST_SUCCESS_KEY = "LAST_SUCCESS";
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String WRITE_LOCK_KEY = "WRITE_LOCK";

  // Populate list and map fields for content comparison
  private static final List<String> LIST_FIELD = ImmutableList.of("1", "2");
  private static final Map<String, String> MAP_FIELD = ImmutableMap.of("1", "2");

  private BucketDataAccessor _bucketDataAccessor;

  @BeforeClass
  public void beforeClass() {
    _bucketDataAccessor = new ZkBucketDataAccessor(ZK_ADDR);
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
    _bucketDataAccessor.compressedBucketDelete(PATH);
  }

  /**
   * Do 10 writes and check that there are 5 versions of the data.
   */
  @Test(dependsOnMethods = "testCompressedBucketRead")
  public void testManyWritesWithVersionCounts() throws IOException {
    int bucketSize = 50 * 1024;
    int numVersions = 5;
    int expectedLastSuccessfulIndex = 4;
    String path = PATH + "2";
    ZNRecord record = new ZNRecord(NAME_KEY);
    record.setSimpleField(NAME_KEY, NAME_KEY);
    record.setListField(NAME_KEY, LIST_FIELD);
    record.setMapField(NAME_KEY, MAP_FIELD);

    BucketDataAccessor bucketDataAccessor =
        new ZkBucketDataAccessor(ZK_ADDR, bucketSize, numVersions);
    for (int i = 0; i < 10; i++) {
      bucketDataAccessor.compressedBucketWrite(path, new HelixProperty(record));
    }

    // Check that there are numVersions number of children under path
    List<String> children = _baseAccessor.getChildNames(path, AccessOption.PERSISTENT);
    Assert.assertEquals(children.size(), numVersions);

    // Check that last successful index is 4 (since we did 10 writes)
    ZNRecord metadata = _baseAccessor.get(path, null, AccessOption.PERSISTENT);
    Assert.assertEquals(metadata.getIntField(LAST_SUCCESS_KEY, -1), expectedLastSuccessfulIndex);

    // Clean up
    bucketDataAccessor.compressedBucketDelete(path);
  }

  /**
   * Write a HelixProperty with large number of entries using BucketDataAccessor and read it back.
   */
  @Test(dependsOnMethods = "testManyWritesWithVersionCounts")
  public void testLargeWriteAndRead() throws IOException {
    String name = "largeResourceAssignment";
    HelixProperty property = createLargeHelixProperty(name, 100000);

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

  /**
   * Tests that each write cleans up previous bucketed data. This method writes some small amount of
   * data and checks that the data buckets from the large write performed in the previous test
   * method have been cleaned up.
   * @throws IOException
   */
  @Test(dependsOnMethods = "testLargeWriteAndRead")
  public void testCleanupBeforeWrite() throws IOException {
    // Create a HelixProperty of a very small size with the same name as the large HelixProperty
    // created from the previous method
    String name = "largeResourceAssignment";
    HelixProperty property = new HelixProperty(name);
    property.getRecord().setIntField("Hi", 10);

    // Get the bucket count from the write performed in the previous method
    ZNRecord metadata = _baseAccessor.get("/" + name, null, AccessOption.PERSISTENT);
    int origBucketSize = metadata.getIntField(BUCKET_SIZE_KEY, -1);

    // Perform a write twice to overwrite both versions
    _bucketDataAccessor.compressedBucketWrite("/" + name, property);
    _bucketDataAccessor.compressedBucketWrite("/" + name, property);

    // Check that the children count for version 0 (version for the large write) is 1
    Assert.assertEquals(
        _baseAccessor.getChildNames("/" + name + "/0", AccessOption.PERSISTENT).size(), 1);

    // Clean up
    _bucketDataAccessor.compressedBucketDelete("/" + name);
  }

  /**
   * Test that no concurrent reads and writes are allowed by triggering multiple operations after
   * creating an artificial lock.
   * @throws IOException
   */
  @Test(dependsOnMethods = "testCleanupBeforeWrite")
  public void testFailureToAcquireLock() throws Exception {
    String name = "acquireLock";
    // Use a large HelixProperty to simulate a write that keeps the lock for some time
    HelixProperty property = createLargeHelixProperty(name, 100);

    // Artificially create the ephemeral ZNode
    _baseAccessor.create("/" + name + "/" + WRITE_LOCK_KEY, new ZNRecord(name),
        AccessOption.EPHEMERAL);

    // Test write
    try {
      _bucketDataAccessor.compressedBucketWrite("/" + name, property);
      Assert.fail("Should fail due to an already-existing lock ZNode!");
    } catch (HelixException e) {
      // Expect an exception
    }

    // Test read
    try {
      _bucketDataAccessor.compressedBucketRead("/" + name, HelixProperty.class);
      Assert.fail("Should fail due to an already-existing lock ZNode!");
    } catch (HelixException e) {
      // Expect an exception
    }

    // Clean up
    _bucketDataAccessor.compressedBucketDelete("/" + name);
  }

  private HelixProperty createLargeHelixProperty(String name, int numEntries) {
    HelixProperty property = new HelixProperty(name);
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
    return property;
  }
}
