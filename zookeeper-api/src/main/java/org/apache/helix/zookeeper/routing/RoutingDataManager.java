package org.apache.helix.zookeeper.routing;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.datamodel.MetadataStoreRoutingData;
import org.apache.helix.msdcommon.datamodel.TrieRoutingData;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.helix.zookeeper.impl.client.SharedZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RoutingDataManager is a Singleton that
 * 1. resolves RoutingDataReader based on the system config given
 * 2. caches routing data
 * 3. provides public methods for reading routing data from various sources (configurable)
 */
public class RoutingDataManager {
  private static Logger LOG = LoggerFactory.getLogger(RoutingDataManager.class);

  /** HTTP call to MSDS is used to fetch routing data by default */
  private String _defaultMsdsEndpoint =
      System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);

  /** Double-checked locking requires that the following fields be final (volatile) */
  // The following map stands for (RoutingDataReaderType_endpoint ID, Raw Routing Data)
  private final Map<String, Map<String, List<String>>> _rawRoutingDataMap =
      new ConcurrentHashMap<>();
  // The following map stands for (RoutingDataReaderType_endpoint ID, MetadataStoreRoutingData)
  private final Map<String, MetadataStoreRoutingData> _metadataStoreRoutingDataMap =
      new ConcurrentHashMap<>();

  // Tracks the time at which reset() was called last. Used to throttle reset()
  private volatile long _lastResetTimestamp;

  // Singleton instance
  private static RoutingDataManager _instance;

  /**
   * This class is a Singleton.
   */
  private RoutingDataManager() {
    // Private constructor for Singleton
  }

  /**
   * Lazy initialization with double-checked locking.
   * @return
   */
  public static RoutingDataManager getInstance() {
    if (_instance == null) {
      synchronized (RoutingDataManager.class) {
        if (_instance == null) {
          _instance = new RoutingDataManager();
        }
      }
    }
    return _instance;
  }

  /**
   * Fetches routing data from the data source via HTTP by querying the MSDS configured in the JVM
   * config.
   * @return
   */
  public Map<String, List<String>> getRawRoutingData() {
    if (_defaultMsdsEndpoint == null || _defaultMsdsEndpoint.isEmpty()) {
      throw new IllegalStateException(
          "HttpRoutingDataReader was unable to find a valid MSDS endpoint String in System "
              + "Properties!");
    }
    return getRawRoutingData(RoutingDataReaderType.HTTP, _defaultMsdsEndpoint);
  }

  /**
   * Fetches routing data from the data source via HTTP.
   * @return a mapping from "metadata store realm addresses" to lists of
   * "metadata store sharding keys", where the sharding keys in a value list all route to
   * the realm address in the key disallows a meaningful mapping to be returned.
   * @param routingDataReaderType
   * @param endpoint
   */
  public Map<String, List<String>> getRawRoutingData(RoutingDataReaderType routingDataReaderType,
      String endpoint) {
    String routingDataCacheKey = getRoutingDataCacheKey(routingDataReaderType, endpoint);
    Map<String, List<String>> rawRoutingData = _rawRoutingDataMap.get(routingDataCacheKey);
    if (rawRoutingData == null) {
      synchronized (RoutingDataManager.class) {
        rawRoutingData = _rawRoutingDataMap.get(routingDataCacheKey);
        if (rawRoutingData == null) {
          RoutingDataReader reader = resolveRoutingDataReader(routingDataReaderType);
          rawRoutingData = reader.getRawRoutingData(endpoint);
          _rawRoutingDataMap.put(routingDataCacheKey, rawRoutingData);
        }
      }
    }
    return rawRoutingData;
  }

  /**
   * Returns the routing data read from MSDS in a MetadataStoreRoutingData format by querying the
   * MSDS configured in the JVM config.
   * @return MetadataStoreRoutingData
   */
  public MetadataStoreRoutingData getMetadataStoreRoutingData() throws InvalidRoutingDataException {
    if (_defaultMsdsEndpoint == null || _defaultMsdsEndpoint.isEmpty()) {
      throw new IllegalStateException(
          "HttpRoutingDataReader was unable to find a valid MSDS endpoint String in System "
              + "Properties!");
    }
    return getMetadataStoreRoutingData(RoutingDataReaderType.HTTP, _defaultMsdsEndpoint);
  }

  /**
   * Returns the routing data read from MSDS as a MetadataStoreRoutingData object.
   * @param routingDataReaderType
   * @param endpoint
   * @return
   * @throws IOException
   * @throws InvalidRoutingDataException
   */
  public MetadataStoreRoutingData getMetadataStoreRoutingData(
      RoutingDataReaderType routingDataReaderType, String endpoint)
      throws InvalidRoutingDataException {
    String routingDataCacheKey = getRoutingDataCacheKey(routingDataReaderType, endpoint);
    MetadataStoreRoutingData metadataStoreRoutingData =
        _metadataStoreRoutingDataMap.get(routingDataCacheKey);
    if (metadataStoreRoutingData == null) {
      synchronized (RoutingDataManager.class) {
        metadataStoreRoutingData = _metadataStoreRoutingDataMap.get(routingDataCacheKey);
        if (metadataStoreRoutingData == null) {
          metadataStoreRoutingData =
              new TrieRoutingData(getRawRoutingData(routingDataReaderType, endpoint));
          _metadataStoreRoutingDataMap.put(routingDataCacheKey, metadataStoreRoutingData);
        }
      }
    }
    return metadataStoreRoutingData;
  }

  /**
   * Clears the statically-cached routing data and private fields.
   */
  public synchronized void reset() {
    _rawRoutingDataMap.clear();
    _metadataStoreRoutingDataMap.clear();
    _defaultMsdsEndpoint =
        System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
    _lastResetTimestamp = System.currentTimeMillis();
  }

  /**
   * Returns the timestamp for the last reset().
   * @return
   */
  public long getLastResetTimestamp() {
    return _lastResetTimestamp;
  }

  /**
   * Returns an appropriate instance of RoutingDataReader given the type.
   * @param routingDataReaderType
   * @return
   */
  private RoutingDataReader resolveRoutingDataReader(RoutingDataReaderType routingDataReaderType) {
    // Instantiate an instance of routing data reader using the type
    try {
      return (RoutingDataReader) Class.forName(routingDataReaderType.getClassName()).newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      String errMsg =
          "RoutingDataManager::resolveRoutingDataReader: failed to instantiate RoutingDataReader! "
              + "RoutingDataReaderType: " + routingDataReaderType;
      LOG.error(errMsg, e);
      throw new MultiZkException(errMsg, e);
    }
  }

  /**
   * Constructs a key for the cache lookup.
   * @param routingDataReaderType
   * @param endpoint
   * @return
   */
  private String getRoutingDataCacheKey(RoutingDataReaderType routingDataReaderType,
      String endpoint) {
    if (routingDataReaderType == null) {
      throw new MultiZkException("RoutingDataManager: RoutingDataReaderType cannot be null!");
    }
    return routingDataReaderType.name() + "_" + endpoint;
  }
}