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
import java.util.List;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
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
  // TODO: Optimize serialization with Jackson
  // TODO: Or use a better binary serialization protocol
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String DATA_SIZE_KEY = "DATA_SIZE";

  // 100 KB for default bucket size
  private static final int DEFAULT_BUCKET_SIZE = 100 * 1024;
  private final int _bucketSize;
  private ZkSerializer _zkSerializer;
  private BaseDataAccessor _zkBaseDataAccessor;
  private BaseDataAccessor<ZNRecord> _znRecordBaseDataAccessor;

  /**
   * Constructor that allows a custom bucket size.
   * @param zkAddr
   * @param bucketSize
   */
  public ZkBucketDataAccessor(String zkAddr, int bucketSize) {
    // There are two HelixZkClients:
    // 1. _zkBaseDataAccessor for writes of binary data
    // 2. _znRecordBaseDataAccessor for writes of ZNRecord (metadata)
    HelixZkClient zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
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
    _zkBaseDataAccessor = new ZkBaseDataAccessor(zkClient);

    // TODO: Consider making this also binary
    // TODO: Consider an async write for the metadata as well
    HelixZkClient znRecordClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    _znRecordBaseDataAccessor = new ZkBaseDataAccessor<>(znRecordClient);
    znRecordClient.setZkSerializer(new ZNRecordSerializer());

    _zkSerializer = new ZNRecordJacksonSerializer();
    _bucketSize = bucketSize;
  }

  /**
   * Constructor that uses a default bucket size.
   * @param zkAddr
   */
  public ZkBucketDataAccessor(String zkAddr) {
    this(zkAddr, DEFAULT_BUCKET_SIZE);
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
    ZNRecord metadataRecord = new ZNRecord(extractIdFromPath(path));
    metadataRecord.setIntField(BUCKET_SIZE_KEY, _bucketSize);
    metadataRecord.setLongField(DATA_SIZE_KEY, compressedRecord.length);
    if (!_znRecordBaseDataAccessor.set(path, metadataRecord, AccessOption.PERSISTENT)) {
      throw new HelixException(String.format("Failed to write the metadata at path: %s!", path));
    }

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
    boolean[] success = _zkBaseDataAccessor.setChildren(paths, buckets, AccessOption.PERSISTENT);

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
  public <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
      Class<T> helixPropertySubType) {
    return helixPropertySubType.cast(compressedBucketRead(path));
  }

  private HelixProperty compressedBucketRead(String path) {
    // Retrieve the metadata
    ZNRecord metadataRecord = _znRecordBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
    if (metadataRecord == null) {
      throw new HelixException(
          String.format("Metadata ZNRecord does not exist for path: %s", path));
    }
    int bucketSize = metadataRecord.getIntField(BUCKET_SIZE_KEY, -1);
    int dataSize = metadataRecord.getIntField(DATA_SIZE_KEY, -1);
    if (bucketSize == -1) {
      throw new HelixException(
          String.format("Metadata ZNRecord does not have %s! Path: %s", BUCKET_SIZE_KEY, path));
    }
    if (dataSize == -1) {
      throw new HelixException(
          String.format("Metadata ZNRecord does not have %s! Path: %s", DATA_SIZE_KEY, path));
    }

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

  /**
   * Returns the last string element in a split String array by /.
   * @param path
   * @return
   */
  private String extractIdFromPath(String path) {
    String[] splitPath = path.split("/");
    return splitPath[splitPath.length - 1];
  }
}
