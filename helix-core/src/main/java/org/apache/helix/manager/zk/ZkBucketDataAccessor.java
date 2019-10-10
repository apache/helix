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

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.AccessOption;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.client.DedicatedZkClientFactory;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.util.GZipCompressionUtil;
import org.codehaus.jackson.map.ObjectMapper;

public class ZkBucketDataAccessor implements BucketDataAccessor, AutoCloseable {
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String DATA_SIZE_KEY = "DATA_SIZE";
  private static final String METADATA_KEY = "METADATA";
  private static final String LAST_SUCCESSFUL_WRITE_KEY = "LAST_SUCCESSFUL_WRITE";
  private static final String LAST_WRITE_KEY = "LAST_WRITE";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  // Thread pool for deleting stale versions
  private static final ExecutorService GC_THREAD_POOL = Executors.newFixedThreadPool(1);

  // 100 KB for default bucket size
  private static final int DEFAULT_BUCKET_SIZE = 50 * 1024;
  private final int _bucketSize;
  private ZkSerializer _zkSerializer;
  private HelixZkClient _zkClient;
  private ZkBaseDataAccessor<byte[]> _zkBaseDataAccessor;

  /**
   * Constructor that allows a custom bucket size.
   * @param zkAddr
   * @param bucketSize
   */
  public ZkBucketDataAccessor(String zkAddr, int bucketSize) {
    _zkClient = DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    _zkClient.setZkSerializer(new ZkSerializer() {
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
    _zkBaseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
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
    DataUpdater<byte[]> lastWriteVersionUpdater = dataInZk -> {
      if (dataInZk == null || dataInZk.length == 0) {
        // No last write version exists, so start with 0
        return Longs.toByteArray(0);
      }
      // Last write exists, so increment and write it back
      long lastWriteVersion = Longs.fromByteArray(dataInZk);
      lastWriteVersion++;
      return Longs.toByteArray(lastWriteVersion);
    };

    // 1. Increment lastWriteVersion using DataUpdater
    ZkBaseDataAccessor.AccessResult result = _zkBaseDataAccessor
        .doUpdate(path + "/" + LAST_WRITE_KEY, lastWriteVersionUpdater, AccessOption.PERSISTENT);
    if (result._retCode != ZkBaseDataAccessor.RetCode.OK) {
      throw new HelixException(
          String.format("Failed to write the write version at path: %s!", path));
    }

    // Successfully reserved a version number
    byte[] binaryVersion = (byte[]) result._updatedValue;
    final long version = Longs.fromByteArray(binaryVersion);

    // 2. Write to the incremented last write version
    String versionedDataPath = path + "/" + version;

    // Take the ZNrecord and serialize it (get byte[])
    byte[] serializedRecord = _zkSerializer.serialize(value.getRecord());
    // Compress the byte[]
    byte[] compressedRecord = GZipCompressionUtil.compress(serializedRecord);
    // Compute N - number of buckets
    int numBuckets = (compressedRecord.length + _bucketSize - 1) / _bucketSize;

    List<String> paths = new ArrayList<>();
    List<byte[]> buckets = new ArrayList<>();

    int ptr = 0;
    int counter = 0;
    while (counter < numBuckets) {
      paths.add(versionedDataPath + "/" + counter);
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

    // 3. Include the metadata in the batch write
    Map<String, String> metadata = ImmutableMap.of(BUCKET_SIZE_KEY, Integer.toString(_bucketSize),
        DATA_SIZE_KEY, Integer.toString(compressedRecord.length));
    byte[] binaryMetadata = OBJECT_MAPPER.writeValueAsBytes(metadata);
    paths.add(path + "/" + version + "/" + METADATA_KEY);
    buckets.add(binaryMetadata);

    // Do an async set to ZK
    boolean[] success = _zkBaseDataAccessor.setChildren(paths, buckets, AccessOption.PERSISTENT);
    // Exception and fail the write if any failed
    for (boolean s : success) {
      if (!s) {
        throw new HelixException(
            String.format("Failed to write the data buckets for path: %s", path));
      }
    }

    // 4. Update lastSuccessfulWriteVersion using Updater
    DataUpdater<byte[]> lastSuccessfulWriteVersionUpdater = dataInZk -> {
      if (dataInZk == null || dataInZk.length == 0) {
        // No last write version exists, so write version from this write
        return Longs.toByteArray(version);
      }
      // Last successful write exists so check if it's smaller than my number
      long lastWriteVersion = Longs.fromByteArray(dataInZk);
      if (lastWriteVersion < version) {
        // Smaller, so I can overwrite
        return Longs.toByteArray(version);
      } else {
        // Greater, I have lagged behind. Return null and do not write
        return null;
      }
    };
    if (!_zkBaseDataAccessor.update(path + "/" + LAST_SUCCESSFUL_WRITE_KEY,
        lastSuccessfulWriteVersionUpdater, AccessOption.PERSISTENT)) {
      throw new HelixException(
          String.format("Failed to write the last successful write metadata at path: %s!", path));
    }

    // 5. Garbage collect stale versions
    GC_THREAD_POOL.submit(() -> deleteStaleVersions(path, Long.toString(version)));
    return true;
  }

  @Override
  public <T extends HelixProperty> HelixProperty compressedBucketRead(String path,
      Class<T> helixPropertySubType) {
    return helixPropertySubType.cast(compressedBucketRead(path));
  }

  @Override
  public void compressedBucketDelete(String path) {
    if (!_zkBaseDataAccessor.remove(path, AccessOption.PERSISTENT)) {
      throw new HelixException(String.format("Failed to delete the bucket data! Path: %s", path));
    }
  }

  @Override
  public void disconnect() {
    if (!_zkClient.isClosed()) {
      _zkClient.close();
    }
  }

  private HelixProperty compressedBucketRead(String path) {
    // 1. Get the version to read
    byte[] binaryVersionToRead = _zkBaseDataAccessor.get(path + "/" + LAST_SUCCESSFUL_WRITE_KEY,
        null, AccessOption.PERSISTENT);
    if (binaryVersionToRead == null) {
      throw new ZkNoNodeException(
          String.format("Last successful write ZNode does not exist for path: %s", path));
    }
    long versionToRead = Longs.fromByteArray(binaryVersionToRead);

    // 2. Get the metadata map
    byte[] binaryMetadata = _zkBaseDataAccessor.get(path + "/" + versionToRead + "/" + METADATA_KEY,
        null, AccessOption.PERSISTENT);
    if (binaryMetadata == null) {
      throw new ZkNoNodeException(
          String.format("Metadata ZNode does not exist for path: %s", path));
    }
    Map metadata;
    try {
      metadata = OBJECT_MAPPER.readValue(binaryMetadata, Map.class);
    } catch (IOException e) {
      throw new HelixException(String.format("Failed to deserialize path metadata: %s!", path), e);
    }

    // 3. Read the data
    Object bucketSizeObj = metadata.get(BUCKET_SIZE_KEY);
    Object dataSizeObj = metadata.get(DATA_SIZE_KEY);
    if (bucketSizeObj == null) {
      throw new HelixException(
          String.format("Metadata ZNRecord does not have %s! Path: %s", BUCKET_SIZE_KEY, path));
    }
    if (dataSizeObj == null) {
      throw new HelixException(
          String.format("Metadata ZNRecord does not have %s! Path: %s", DATA_SIZE_KEY, path));
    }
    int bucketSize = Integer.parseInt((String) bucketSizeObj);
    int dataSize = Integer.parseInt((String) dataSizeObj);

    // Compute N - number of buckets
    int numBuckets = (dataSize + _bucketSize - 1) / _bucketSize;
    byte[] compressedRecord = new byte[dataSize];
    String dataPath = path + "/" + versionToRead;

    List<String> paths = new ArrayList<>();
    for (int i = 0; i < numBuckets; i++) {
      paths.add(dataPath + "/" + i);
    }

    // Async get
    List<byte[]> buckets = _zkBaseDataAccessor.get(paths, null, AccessOption.PERSISTENT, true);

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
  public void close() {
    disconnect();
  }

  /**
   * Deletes all versions that are older than the currentVersion given.
   * @param path
   * @param currentVersion
   */
  private void deleteStaleVersions(String path, String currentVersion) {
    // Get all children names under path
    List<String> children = _zkBaseDataAccessor.getChildNames(path, AccessOption.PERSISTENT);
    if (children == null || children.isEmpty()) {
      // The whole path has been deleted so return immediately
      return;
    }
    filterChildrenNames(children, currentVersion);
    List<String> pathsToDelete = getPathsToDelete(path, children);
    for (String pathToDelete : pathsToDelete) {
      // TODO: Should be batch delete but it doesn't work. It's okay since this runs async
      _zkBaseDataAccessor.remove(pathToDelete, AccessOption.PERSISTENT);
    }
  }

  /**
   * Filter out currentVersion and non-version children names from the children list given.
   * @param children
   * @param currentVersion
   */
  private void filterChildrenNames(List<String> children, String currentVersion) {
    children.remove(LAST_SUCCESSFUL_WRITE_KEY);
    children.remove(LAST_WRITE_KEY);
    children.remove(currentVersion);
  }

  /**
   * Generates all stale paths to delete.
   * @param path
   * @param staleVersions
   * @return
   */
  private List<String> getPathsToDelete(String path, List<String> staleVersions) {
    List<String> pathsToDelete = new ArrayList<>();
    staleVersions.forEach(ver -> pathsToDelete.add(path + "/" + ver));
    return pathsToDelete;
  }
}
