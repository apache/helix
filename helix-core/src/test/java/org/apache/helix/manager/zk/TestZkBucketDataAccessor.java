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
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestZkBucketDataAccessor extends ZkTestBase {
  private static final String PATH = "/" + TestHelper.getTestClassName();
  private static final String NAME_KEY = TestHelper.getTestClassName();
  private static final String LAST_SUCCESSFUL_WRITE_KEY = "LAST_SUCCESSFUL_WRITE";
  private static final String LAST_WRITE_KEY = "LAST_WRITE";

  // Populate list and map fields for content comparison
  private static final List<String> LIST_FIELD = ImmutableList.of("1", "2");
  private static final Map<String, String> MAP_FIELD = ImmutableMap.of("1", "2");

  private BucketDataAccessor _bucketDataAccessor;
  private BaseDataAccessor<byte[]> _zkBaseDataAccessor;

  private ZNRecord record = new ZNRecord(NAME_KEY);

  @BeforeClass
  public void beforeClass() {
    // Initialize ZK accessors for testing
    _bucketDataAccessor = new ZkBucketDataAccessor(ZK_ADDR);
    HelixZkClient zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));
    zkClient.setZkSerializer(new ZkSerializer() {
      @Override
      public byte[] serialize(Object data) throws ZkMarshallingError {
        if (data instanceof byte[]) {
          return (byte[]) data;
        }
        throw new HelixException("ZkBucketDataAccesor only supports a byte array as an argument!");
      }

      @Override
      public Object deserialize(byte[] data) throws ZkMarshallingError {
        return data;
      }
    });
    _zkBaseDataAccessor = new ZkBaseDataAccessor<>(zkClient);

    // Fill in some data for the record
    record.setSimpleField(NAME_KEY, NAME_KEY);
    record.setListField(NAME_KEY, LIST_FIELD);
    record.setMapField(NAME_KEY, MAP_FIELD);
  }

  @AfterClass
  public void afterClass() {
    _bucketDataAccessor.disconnect();
  }

  /**
   * Attempt writing a simple HelixProperty using compressedBucketWrite.
   * @throws IOException
   */
  @Test
  public void testCompressedBucketWrite() throws IOException {
    Assert.assertTrue(_bucketDataAccessor.compressedBucketWrite(PATH, new HelixProperty(record)));
  }

  @Test(dependsOnMethods = "testCompressedBucketWrite")
  public void testMultipleWrites() throws Exception {
    int count = 50;

    // Write 10 times
    for (int i = 0; i < count; i++) {
      _bucketDataAccessor.compressedBucketWrite(PATH, new HelixProperty(record));
    }

    // Last known good version number should be "count"
    byte[] binarySuccessfulWriteVer = _zkBaseDataAccessor
        .get(PATH + "/" + LAST_SUCCESSFUL_WRITE_KEY, null, AccessOption.PERSISTENT);
    long lastSuccessfulWriteKey = Longs.fromByteArray(binarySuccessfulWriteVer);
    Assert.assertEquals(lastSuccessfulWriteKey, count);

    // Last write version should be "count"
    byte[] binaryWriteVer =
        _zkBaseDataAccessor.get(PATH + "/" + LAST_WRITE_KEY, null, AccessOption.PERSISTENT);
    long writeVer = Longs.fromByteArray(binaryWriteVer);
    Assert.assertEquals(writeVer, count);

    // Test that all previous versions have been deleted
    // Use Verifier because GC can take ZK delay
    Assert.assertTrue(TestHelper.verify(() -> {
      List<String> children = _zkBaseDataAccessor.getChildNames(PATH, AccessOption.PERSISTENT);
      return children.size() == 3;
    }, 60 * 1000L));
  }

  /**
   * The record written in {@link #testCompressedBucketWrite()} is the same record that was written.
   */
  @Test(dependsOnMethods = "testMultipleWrites")
  public void testCompressedBucketRead() {
    HelixProperty readRecord = _bucketDataAccessor.compressedBucketRead(PATH, HelixProperty.class);
    Assert.assertEquals(readRecord.getRecord().getSimpleField(NAME_KEY), NAME_KEY);
    Assert.assertEquals(readRecord.getRecord().getListField(NAME_KEY), LIST_FIELD);
    Assert.assertEquals(readRecord.getRecord().getMapField(NAME_KEY), MAP_FIELD);
    _bucketDataAccessor.compressedBucketDelete(PATH);
  }

  /**
   * Write a HelixProperty with large number of entries using BucketDataAccessor and read it back.
   */
  @Test(dependsOnMethods = "testCompressedBucketRead")
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
