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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.util.GZipCompressionUtil;
import org.codehaus.jackson.map.ObjectMapper;

public class ZkBucketDataAccessor implements BucketDataAccessor {

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String DATA_SIZE_KEY = "DATA_SIZE";

  // 100 KB for default bucket size
  private static final int DEFAULT_BUCKET_SIZE = 100 * 1024;
  private int _bucketSize = DEFAULT_BUCKET_SIZE;
  private ZkSerializer _zkSerializer;
  private BaseDataAccessor _zkBaseDataAccessor;

  public ZkBucketDataAccessor(String zkAddr) {
    HelixZkClient zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    zkClient.setZkSerializer(new DummySerializer());
    _zkSerializer = new ZNRecordJacksonSerializer();
    _zkBaseDataAccessor = new ZkBaseDataAccessor(zkClient);
  }

  @Override
  public <T extends HelixProperty> boolean bucketWrite(String path, T value) {
    throw new UnsupportedOperationException("bucketWrite not supported!");
  }

  @Override
  public HelixProperty bucketRead(String path) {
    throw new UnsupportedOperationException("bucketRead not supported!");
  }

  @Override
  public <T extends HelixProperty> boolean compressedBucketWrite(String path, T value)
      throws IOException {
    // Take the ZNrecord and serialize it (get byte[])
    byte[] serializedRecord = _zkSerializer.serialize(value.getRecord());

    // Compress the byte[]
    byte[] compressedRecord = GZipCompressionUtil.compress(serializedRecord);

    // Compute N - number of buckets
    int numBuckets = (compressedRecord.length + _bucketSize - 1) / _bucketSize;

    List<String> paths = new ArrayList<>();
    List<Object> buckets = new ArrayList<>();

    // Add the metadata ZNode first
    paths.add(path);
    Map<String, Integer> metadata = new HashMap<>();
    metadata.put(BUCKET_SIZE_KEY, _bucketSize);
    metadata.put(DATA_SIZE_KEY, compressedRecord.length);
    buckets.add(OBJECT_MAPPER.writeValueAsBytes(metadata));

    int ptr = 0;
    int counter = 0;
    while (counter < numBuckets) {
      paths.add(path + "/" + counter);
      if (counter == numBuckets - 1) {
        // Special treatment for the last bucket
        buckets.add(
            Arrays.copyOfRange(compressedRecord, ptr, ptr + compressedRecord.length % _bucketSize));
      } else {
        buckets.add(Arrays.copyOfRange(compressedRecord, ptr, ptr + _bucketSize));
      }
      ptr += _bucketSize;
      counter++;
    }

    // Do an async set to ZK
    boolean[] success;
    success = _zkBaseDataAccessor.setChildren(paths, buckets, AccessOption.PERSISTENT);

    // Return false if any of the writes failed
    // TODO: Improve the failure handling
    for (boolean s : success) {
      if (!s) {
        return false;
      }
    }
    return true;
  }

  @Override
  public HelixProperty compressedBucketRead(String path) {
    // Retrieve the metadata
    Map metadataMap;
    byte[] metadata = (byte[]) _zkBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
    try {
      metadataMap = OBJECT_MAPPER.readValue(metadata, Map.class);
    } catch (IOException e) {
      throw new HelixException(String.format("Failed to read the metadata for path: %s!", path), e);
    }
    int bucketSize = (int) metadataMap.get(BUCKET_SIZE_KEY);
    int dataSize = (int) metadataMap.get(DATA_SIZE_KEY);

    // Compute N - number of buckets
    int numBuckets = (dataSize + _bucketSize - 1) / _bucketSize;

    byte[] compressedRecord = new byte[dataSize];

    List<String> paths = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      paths.add(path + "/" + i);
    }

    // Async get
    List buckets = _zkBaseDataAccessor.get(paths, null, AccessOption.PERSISTENT, true);

    // Combine buckets into one byte array
    int copyPtr = 0;
    for (int i = 0; i < numBuckets; i++) {
      if (i == numBuckets - 1) {
        // Special treatment for the last bucket
        System.arraycopy(buckets.get(i), 0, compressedRecord, copyPtr, dataSize % bucketSize);
      } else {
        System.arraycopy(buckets.get(i), 0, compressedRecord, copyPtr, bucketSize);
        copyPtr += bucketSize;
      }
    }

    // Decompress the byte array
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedRecord);
    byte[] serializedRecord;
    try {
      serializedRecord = GZipCompressionUtil.uncompress(byteArrayInputStream);
    } catch (IOException e) {
      throw new HelixException(String.format("Failed to decompress path: %s!", path), e);
    }

    // Deserialize the record to retrieve the original
    ZNRecord originalRecord = (ZNRecord) _zkSerializer.deserialize(serializedRecord);
    return new HelixProperty(originalRecord);
  }

  @Override
  public void setBucketSize(int size) {
    throw new UnsupportedOperationException("setBucketSize not supported!");
  }
}
