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
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.util.GZipCompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkBucketDataAccessor implements BucketDataAccessor {
  private static Logger LOG = LoggerFactory.getLogger(ZkBucketDataAccessor.class);

  private static final int DEFAULT_NUM_VERSIONS = 2;
  private static final String BUCKET_SIZE_KEY = "BUCKET_SIZE";
  private static final String DATA_SIZE_KEY = "DATA_SIZE";
  private static final String WRITE_LOCK_KEY = "WRITE_LOCK";
  private static final String LAST_SUCCESS_KEY = "LAST_SUCCESS";

  // 100 KB for default bucket size
  private static final int DEFAULT_BUCKET_SIZE = 50 * 1024;
  private final int _bucketSize;
  private final int _numVersions;
  private ZkSerializer _zkSerializer;
  private HelixZkClient _zkClient;
  private HelixZkClient _znRecordClient;
  private BaseDataAccessor _zkBaseDataAccessor;
  private BaseDataAccessor<ZNRecord> _znRecordBaseDataAccessor;

  /**
   * Constructor that allows a custom bucket size.
   * @param zkAddr
   * @param bucketSize
   * @param numVersions number of versions to store in ZK
   */
  public ZkBucketDataAccessor(String zkAddr, int bucketSize, int numVersions) {
    // There are two HelixZkClients:
    // 1. _zkBaseDataAccessor for writes of binary data
    // 2. _znRecordBaseDataAccessor for writes of ZNRecord (metadata)
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
    _zkBaseDataAccessor = new ZkBaseDataAccessor(_zkClient);

    // TODO: Optimize serialization with Jackson
    // TODO: Or use a better binary serialization protocol
    // TODO: Consider making this also binary
    // TODO: Consider an async write for the metadata as well
    _znRecordClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr));
    _znRecordBaseDataAccessor = new ZkBaseDataAccessor<>(_znRecordClient);
    _znRecordClient.setZkSerializer(new ZNRecordSerializer());

    _zkSerializer = new ZNRecordJacksonSerializer();
    _bucketSize = bucketSize;
    _numVersions = numVersions;
  }

  /**
   * Constructor that uses a default bucket size.
   * @param zkAddr
   */
  public ZkBucketDataAccessor(String zkAddr) {
    this(zkAddr, DEFAULT_BUCKET_SIZE, DEFAULT_NUM_VERSIONS);
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

    if (tryLock(path)) {
      try {
        // Read or initialize metadata and compute the last success version index
        ZNRecord metadataRecord =
            _znRecordBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
        if (metadataRecord == null) {
          metadataRecord = new ZNRecord(extractIdFromPath(path));
        }
        int lastSuccessIndex =
            (metadataRecord.getIntField(LAST_SUCCESS_KEY, -1) + 1) % _numVersions;
        String dataPath = path + "/" + lastSuccessIndex;

        List<String> paths = new ArrayList<>();
        List<Object> buckets = new ArrayList<>();

        int ptr = 0;
        int counter = 0;
        while (counter < numBuckets) {
          paths.add(dataPath + "/" + counter);
          if (counter == numBuckets - 1) {
            // Special treatment for the last bucket
            buckets.add(Arrays.copyOfRange(compressedRecord, ptr,
                ptr + compressedRecord.length % _bucketSize));
          } else {
            buckets.add(Arrays.copyOfRange(compressedRecord, ptr, ptr + _bucketSize));
          }
          ptr += _bucketSize;
          counter++;
        }

        // Do a cleanup of previous data
        if (!_zkBaseDataAccessor.remove(dataPath, AccessOption.PERSISTENT)) {
          // Clean-up is not critical so upon failure, we log instead of throwing an exception
          LOG.warn("Failed to clean up previous bucketed data in data path: {}", dataPath);
        }

        // Do an async set to ZK
        boolean[] success =
            _zkBaseDataAccessor.setChildren(paths, buckets, AccessOption.PERSISTENT);
        // Exception and fail the write if any failed
        for (boolean s : success) {
          if (!s) {
            throw new HelixException(
                String.format("Failed to write the data buckets for path: %s", path));
          }
        }

        // Data write completed, so update the metadata with last success index
        // Note that the metadata ZNodes is written using sync write
        metadataRecord.setIntField(BUCKET_SIZE_KEY, _bucketSize);
        metadataRecord.setLongField(DATA_SIZE_KEY, compressedRecord.length);
        metadataRecord.setIntField(LAST_SUCCESS_KEY, lastSuccessIndex);
        if (!_znRecordBaseDataAccessor.set(path, metadataRecord, AccessOption.PERSISTENT)) {
          throw new HelixException(
              String.format("Failed to write the metadata at path: %s!", path));
        }
      } finally {
        // Critical section for write ends here
        unlock(path);
      }
      return true;
    }
    throw new HelixException(String.format("Could not acquire lock for write. Path: %s", path));
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
    if (!_znRecordClient.isClosed()) {
      _znRecordClient.close();
    }
  }

  private HelixProperty compressedBucketRead(String path) {
    // TODO: Incorporate parallelism into reads instead of locking the whole thing against other
    // reads and writes
    if (tryLock(path)) {
      try {
        // Retrieve the metadata
        ZNRecord metadataRecord =
            _znRecordBaseDataAccessor.get(path, null, AccessOption.PERSISTENT);
        if (metadataRecord == null) {
          throw new HelixException(
              String.format("Metadata ZNRecord does not exist for path: %s", path));
        }

        int bucketSize = metadataRecord.getIntField(BUCKET_SIZE_KEY, -1);
        int dataSize = metadataRecord.getIntField(DATA_SIZE_KEY, -1);
        int lastSuccessIndex = metadataRecord.getIntField(LAST_SUCCESS_KEY, -1);
        if (lastSuccessIndex == -1) {
          throw new HelixException(String.format("Metadata ZNRecord does not have %s! Path: %s",
              LAST_SUCCESS_KEY, path));
        }
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
        String dataPath = path + "/" + lastSuccessIndex;

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
          paths.add(dataPath + "/" + i);
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
      } finally {
        // Critical section for read ends here
        unlock(path);
      }
    }
    throw new HelixException(String.format("Could not acquire lock for read. Path: %s", path));
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

  /**
   * Acquires the lock (create an ephemeral node) only if it is free (no ephemeral node already
   * exists) at the time of invocation.
   * @param path
   * @return
   */
  private boolean tryLock(String path) {
    // Check if another write is taking place and if not, create an ephemeral node to simulate
    // acquiring of a lock
    return !_zkBaseDataAccessor.exists(path + "/" + WRITE_LOCK_KEY, AccessOption.EPHEMERAL)
        && _zkBaseDataAccessor.set(path + "/" + WRITE_LOCK_KEY, new byte[0],
            AccessOption.EPHEMERAL);
  }

  /**
   * Releases the lock (removes the ephemeral node).
   * @param path
   */
  private void unlock(String path) {
    // Write succeeded, so release the lock
    if (!_zkBaseDataAccessor.remove(path + "/" + WRITE_LOCK_KEY, AccessOption.EPHEMERAL)) {
      throw new HelixException(String.format("Could not remove ephemeral node for path: %s", path));
    }
    // TODO: In case of remove failure, we risk a lock never getting released.
    // TODO: Consider two possible improvements
    // TODO: 1. Use ephemeral owner id for the same connection to reclaim the lock
    // TODO: 2. Use "lease" - lock with a timeout
  }
}
