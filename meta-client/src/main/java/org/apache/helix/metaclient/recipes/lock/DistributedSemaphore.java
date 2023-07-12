package org.apache.helix.metaclient.recipes.lock;

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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.exception.MetaClientException;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientConfig;
import org.apache.helix.metaclient.impl.zk.factory.ZkMetaClientFactory;
import org.apache.helix.metaclient.impl.zk.util.ZkMetaClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class DistributedSemaphore {
  private final MetaClientInterface<DataRecord> _metaClient;
  private String _path;
  private static final String INITIAL_CAPACITY_NAME = "INITIAL_CAPACITY";
  private static final String REMAINING_CAPACITY_NAME = "REMAINING_CAPACITY";
  private static final long DEFAULT_REMAINING_CAPACITY = -1;
  private static final Logger LOG = LoggerFactory.getLogger(DistributedSemaphore.class);

  /**
   * Create a distributed semaphore client with the given configuration.
   * @param config configuration of the client
   */
  public DistributedSemaphore(MetaClientConfig config) {
    if (config == null) {
      throw new MetaClientException("Configuration cannot be null");
    }
    LOG.info("Creating DistributedSemaphore Client");
    if (MetaClientConfig.StoreType.ZOOKEEPER.equals(config.getStoreType())) {
      ZkMetaClientConfig zkMetaClientConfig = new ZkMetaClientConfig.ZkMetaClientConfigBuilder()
          .setConnectionAddress(config.getConnectionAddress())
          .setZkSerializer(new DataRecordSerializer()) // Currently only support ZNRecordSerializer.
          // Setting DataRecordSerializer as DataRecord extends ZNRecord.
          .build();
      _metaClient = new ZkMetaClientFactory().getMetaClient(zkMetaClientConfig);
      _metaClient.connect();
    } else {
      throw new MetaClientException("Unsupported store type: " + config.getStoreType());
    }
  }

  /**
   * Connect to an existing distributed semaphore client.
   * @param client client to connect to
   */
  public DistributedSemaphore(MetaClientInterface<DataRecord> client) {
    if (client == null) {
      throw new MetaClientException("Client cannot be null");
    }
    LOG.info("Connecting to existing DistributedSemaphore Client");
    _metaClient = client;
    if (_metaClient.isClosed()) {
      throw new IllegalStateException("Client already closed!");
    }
    try {
      _metaClient.connect();
    } catch (IllegalStateException e) {
      // Already connected.
    }
  }

  /**
   * Create a distributed semaphore with the given path and capacity.
   * @param path path of the semaphore
   * @param capacity capacity of the semaphore
   */
  public void createSemaphore(String path, int capacity) {
    if (capacity <= 0) {
      throw new MetaClientException("Capacity must be positive");
    }
    if (path == null || path.isEmpty()) {
      throw new MetaClientException("Invalid path to create semaphore");
    }
    if (_metaClient.exists(path) != null) {
      throw new MetaClientException("Semaphore already exists");
    }
    if (_metaClient.exists(path) == null) {
      DataRecord dataRecord = new DataRecord(path);
      dataRecord.setLongField(INITIAL_CAPACITY_NAME, capacity);
      dataRecord.setLongField(REMAINING_CAPACITY_NAME, capacity);
      _metaClient.create(path, dataRecord);
      _path = path;
    }
  }

  /**
   * Connect to an existing distributed semaphore.
   * @param path path of the semaphore
   */
  public void connectSemaphore(String path) {
    if (path == null || path.isEmpty()) {
      throw new MetaClientException("Invalid path to connect semaphore");
    }
    if (_metaClient.exists(path) == null) {
      throw new MetaClientException("Semaphore does not exist");
    }
    _path = path;
  }

  /**
   * Acquire a permit. If no permit is available, log error and return null.
   * @return a permit
   */
  public Permit acquire() {
    try {
      updateAcquirePermit(1);
      return retrievePermit(_path);
    } catch (MetaClientException e) {
      LOG.error("Failed to acquire permit.", e);
      return null;
    }
  }


  /**
   * Try to acquire multiple permits. If not enough permits are available, log error and return null.
   * @param count number of permits to acquire
   * @return a collection of permits
   */
  public Collection<Permit> acquire(int count) {
    try {
      updateAcquirePermit(count);
      Collection<Permit> permits = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        permits.add(retrievePermit(_path));
      }
      return permits;
    } catch (MetaClientException e) {
      LOG.error("Failed to acquire permits.", e);
      return null;
    }
  }

  /**
   * Try to acquire a permit. If no enough permit is available, wait for a specific time or return when it was able to acquire.
   * If timeout <=0, then return immediately when not able to acquire.
   * @param count number of permits to acquire
   * @param timeout time to wait
   * @param unit time unit
   * @return a collection of permits
   */
  public Collection<Permit> acquire(int count, long timeout, TimeUnit unit) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Get the remaining capacity of the semaphore
   * @return remaining capacity
   */
  public long getRemainingCapacity() {
    return getSemaphore().getLongField(REMAINING_CAPACITY_NAME, DEFAULT_REMAINING_CAPACITY);
  }

  /**
   * Get the semaphore data record
   * @return semaphore data record
   */
  private DataRecord getSemaphore() {
    if (_metaClient.exists(_path) == null) {
      throw new MetaClientException("Semaphore does not exist at path: " + _path + ". Please create it first.");
    }
    return new DataRecord(_metaClient.get(_path));
  }

  /**
   * Return a permit. If the permit is already returned, log and return void.
   */
  public void returnPermit(Permit permit) {
    if (permit.isReleased()) {
      LOG.info("The permit has already been released");
    } else {
      updateReturnPermit();
      permit.releasePermit();
    }
  }

  /**
   * Return a collection of permits. If a permit in that collection is already returned,
   * log and return void.
   */
  public void returnAllPermits(Collection<Permit> permits) {
    for (Permit permit : permits) {
      returnPermit(permit);
    }
  }

  /**
   * Retrieve a permit from the semaphore data record.
   * @param path path of the permit
   * @return a permit
   */
  private Permit retrievePermit(String path) {
    MetaClientInterface.Stat stat = _metaClient.exists(path);
    return new Permit(getSemaphore(), stat);
  }

  /**
   * Update the remaining capacity of the semaphore after acquiring a permit.
   * @param count number of permits to acquire
   */
  private void updateAcquirePermit(int count) {
    _metaClient.update(_path, record -> {
      long permitsAvailable = record.getLongField(REMAINING_CAPACITY_NAME, DEFAULT_REMAINING_CAPACITY);
      if (permitsAvailable < count) {
        throw new MetaClientException("No sufficient permits available. Attempt to acquire " + count + " permits, but only "
            + permitsAvailable + " permits available");
      }
      record.setLongField(REMAINING_CAPACITY_NAME, permitsAvailable - count);
      return record;
    });
  }

  /**
   * Update the remaining capacity of the semaphore after returning a permit.
   */
  private void updateReturnPermit() {
    _metaClient.update(_path, record -> {
      long permitsAvailable = record.getLongField(REMAINING_CAPACITY_NAME, DEFAULT_REMAINING_CAPACITY);
      record.setLongField(REMAINING_CAPACITY_NAME, permitsAvailable + 1);
      return record;
    });
  }
}
