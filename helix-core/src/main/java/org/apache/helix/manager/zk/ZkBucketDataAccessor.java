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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ByteArraySerializer;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBucketDataAccessor implements BucketDataAccessor, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZkBucketDataAccessor.class);

  private static final int DEFAULT_BUCKET_SIZE = 50 * 1024; // 50KB
  private static final long DEFAULT_VERSION_TTL = TimeUnit.MINUTES.toMillis(1L); // 1 min
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String DATA_SIZE_KEY = "DATA_SIZE";
  private static final String METADATA_KEY = "METADATA";
  private static final String LAST_SUCCESSFUL_WRITE_KEY = "LAST_SUCCESSFUL_WRITE";
  private static final String LAST_WRITE_KEY = "LAST_WRITE";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  // Thread pool for deleting stale versions
  // Note that newScheduledThreadPool(1) may not work. newSingleThreadScheduledExecutor guarantees
  // sequential execution, which is what we want for implementing TTL & GC here
  private static final ScheduledExecutorService GC_THREAD =
      Executors.newSingleThreadScheduledExecutor((runnable) -> {
        Thread thread = new Thread(runnable, "ZkBucketDataAccessorGcThread");
        thread.setDaemon(true);
        return thread;
      });

  private final int _bucketSize;
  private final long _versionTTLms;
  private final ZkSerializer _zkSerializer;
  private final RealmAwareZkClient _zkClient;
  private final ZkBaseDataAccessor<byte[]> _zkBaseDataAccessor;
  private final Map<String, ScheduledFuture> _gcTaskFutureMap = new ConcurrentHashMap<>();
  private boolean _usesExternalZkClient = false;

  /**
   * Constructor that allows a custom bucket size.
   * @param zkAddr
   * @param bucketSize
   * @param versionTTLms in ms
   */
  public ZkBucketDataAccessor(String zkAddr, int bucketSize, long versionTTLms) {
    this(createRealmAwareZkClient(zkAddr), bucketSize, versionTTLms, false);
  }

  public ZkBucketDataAccessor(RealmAwareZkClient zkClient) {
    this(zkClient, DEFAULT_BUCKET_SIZE, DEFAULT_VERSION_TTL, true);
  }

  public ZkBucketDataAccessor(RealmAwareZkClient zkClient, int bucketSize, long versionTTLms) {
    this(zkClient, bucketSize, versionTTLms, true);
  }

  private ZkBucketDataAccessor(RealmAwareZkClient zkClient, int bucketSize, long versionTTLms,
      boolean usesExternalZkClient) {
    _zkClient = zkClient;
    _zkBaseDataAccessor = new ZkBaseDataAccessor<>(_zkClient);
    _zkSerializer = new ZNRecordJacksonSerializer();
    _bucketSize = bucketSize;
    _versionTTLms = versionTTLms;
    _usesExternalZkClient = usesExternalZkClient;
  }

  /**
   * Constructor that uses a default bucket size.
   * @param zkAddr
   */
  public ZkBucketDataAccessor(String zkAddr) {
    this(zkAddr, DEFAULT_BUCKET_SIZE, DEFAULT_VERSION_TTL);
  }

  private static RealmAwareZkClient createRealmAwareZkClient(String zkAddr) {
    RealmAwareZkClient zkClient;
    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddr == null) {
      LOG.warn("ZkBucketDataAccessor: either multi-zk enabled or zkAddr is null - "
          + "starting ZkBucketDataAccessor in multi-zk mode!");
      try {
        // Create realm-aware ZkClient.
        RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build();
        RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
            new RealmAwareZkClient.RealmAwareZkClientConfig();
        zkClient = new FederatedZkClient(connectionConfig, clientConfig);
      } catch (IllegalArgumentException | InvalidRoutingDataException e) {
        throw new HelixException("Not able to connect on realm-aware mode", e);
      }
    } else {
      zkClient = DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    }
    zkClient.setZkSerializer(new ByteArraySerializer());
    return zkClient;
  }

  @Override
  public <T extends HelixProperty> boolean compressedBucketWrite(String rootPath, T value)
      throws IOException {
    DataUpdater<byte[]> lastWriteVersionUpdater = dataInZk -> {
      if (dataInZk == null || dataInZk.length == 0) {
        // No last write version exists, so start with 0
        return "0".getBytes();
      }
      // Last write exists, so increment and write it back
      // **String conversion is necessary to make it display in ZK (zooinspector)**
      String lastWriteVersionStr = new String(dataInZk);
      long lastWriteVersion = Long.parseLong(lastWriteVersionStr);
      lastWriteVersion++;
      return String.valueOf(lastWriteVersion).getBytes();
    };

    // 1. Increment lastWriteVersion using DataUpdater
    ZkBaseDataAccessor.AccessResult result = _zkBaseDataAccessor.doUpdate(
        rootPath + "/" + LAST_WRITE_KEY, lastWriteVersionUpdater, AccessOption.PERSISTENT);
    if (result._retCode != ZkBaseDataAccessor.RetCode.OK) {
      throw new HelixException(
          String.format("Failed to write the write version at path: %s!", rootPath));
    }

    // Successfully reserved a version number
    byte[] binaryVersion = (byte[]) result._updatedValue;
    String versionStr = new String(binaryVersion);
    final long version = Long.parseLong(versionStr);

    // 2. Write to the incremented last write version
    String versionedDataPath = rootPath + "/" + versionStr;

    // Take the ZNRecord and serialize it (get byte[])
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
    paths.add(versionedDataPath + "/" + METADATA_KEY);
    buckets.add(binaryMetadata);

    // Do an async set to ZK
    boolean[] success = _zkBaseDataAccessor.setChildren(paths, buckets, AccessOption.PERSISTENT);
    // Exception and fail the write if any failed
    for (boolean s : success) {
      if (!s) {
        throw new HelixException(
            String.format("Failed to write the data buckets for path: %s", rootPath));
      }
    }

    // 4. Update lastSuccessfulWriteVersion using Updater
    DataUpdater<byte[]> lastSuccessfulWriteVersionUpdater = dataInZk -> {
      if (dataInZk == null || dataInZk.length == 0) {
        // No last write version exists, so write version from this write
        return versionStr.getBytes();
      }
      // Last successful write exists so check if it's smaller than my number
      String lastWriteVersionStr = new String(dataInZk);
      long lastWriteVersion = Long.parseLong(lastWriteVersionStr);
      if (lastWriteVersion < version) {
        // Smaller, so I can overwrite
        return versionStr.getBytes();
      } else {
        // Greater, I have lagged behind. Return null and do not write
        return null;
      }
    };
    if (!_zkBaseDataAccessor.update(rootPath + "/" + LAST_SUCCESSFUL_WRITE_KEY,
        lastSuccessfulWriteVersionUpdater, AccessOption.PERSISTENT)) {
      throw new HelixException(String
          .format("Failed to write the last successful write metadata at path: %s!", rootPath));
    }

    // 5. Update the timer for GC
    scheduleStaleVersionGC(rootPath);
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
    synchronized (this) {
      _gcTaskFutureMap.remove(path);
    }
  }

  @Override
  public void disconnect() {
    if (!_usesExternalZkClient && _zkClient != null && !_zkClient.isClosed()) {
      _zkClient.close();
    }
  }

  private HelixProperty compressedBucketRead(String path) {
    // 1. Get the version to read
    String versionToRead = getLastSuccessfulWriteVersion(path);

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

  @Override
  public void finalize() {
    _zkBaseDataAccessor.close();
    close();
  }

  private synchronized void scheduleStaleVersionGC(String rootPath) {
    // If GC already scheduled, return early
    if (_gcTaskFutureMap.containsKey(rootPath)) {
      return;
    }
    // Schedule GC task
    _gcTaskFutureMap.put(rootPath, GC_THREAD.schedule(() -> {
        try {
          _gcTaskFutureMap.remove(rootPath);
          deleteStaleVersions(rootPath);
        } catch (Exception ex) {
          LOG.error("Failed to delete the stale versions.", ex);
        }
      }, _versionTTLms, TimeUnit.MILLISECONDS));
    }

  /**
   * Deletes all stale versions.
   * @param rootPath
   */
  private void deleteStaleVersions(String rootPath) {
    // Get most recent write version
    String currentVersionStr = getLastSuccessfulWriteVersion(rootPath);

    // Get all children names under path
    List<String> children = _zkBaseDataAccessor.getChildNames(rootPath, AccessOption.PERSISTENT);
    if (children == null || children.isEmpty()) {
      // The whole path has been deleted so return immediately
      return;
    }
    List<String> pathsToDelete =
        getPathsToDelete(rootPath, filterChildrenNames(children, Long.parseLong(currentVersionStr)));
    for (String pathToDelete : pathsToDelete) {
      // TODO: Should be batch delete but it doesn't work. It's okay since this runs async
      _zkBaseDataAccessor.remove(pathToDelete, AccessOption.PERSISTENT);
    }
  }

  /**
   * Filter out non-version children names and non-stale versions.
   * @param childrenNodes
   * @param currentVersion
   * @return The filtered child node names to be removed
   */
  private List<String> filterChildrenNames(List<String> childrenNodes, long currentVersion) {
    List<String> childrenToRemove = new ArrayList<>();
    for (String child : childrenNodes) {
      // Leave out metadata
      if (child.equals(LAST_SUCCESSFUL_WRITE_KEY) || child.equals(LAST_WRITE_KEY)) {
        continue;
      }
      long childVer;
      try {
        childVer = Long.parseLong(child);
      } catch (NumberFormatException ex) {
        LOG.warn("Found an invalid ZNode: {}", child);
        // Ignore ZNode names that aren't parseable
        continue;
      }
      if (childVer < currentVersion) {
        childrenToRemove.add(child);
      }
      // Leave out currentVersion and above
      // This is because we want to honor the TTL for newer versions
    }
    return childrenToRemove;
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

  private String getLastSuccessfulWriteVersion(String path) {
    byte[] binaryVersionToRead = _zkBaseDataAccessor.get(path + "/" + LAST_SUCCESSFUL_WRITE_KEY,
        null, AccessOption.PERSISTENT);
    if (binaryVersionToRead == null) {
      throw new ZkNoNodeException(
          String.format("Last successful write ZNode does not exist for path: %s", path));
    }
    return new String(binaryVersionToRead);
  }
}
