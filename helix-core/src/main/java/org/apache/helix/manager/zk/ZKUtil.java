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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Using this ZKUtil class for production purposes is NOT recommended since a lot of the static
 * methods require a ZkClient instance to be passed in.
 *
 * NOTE: Ephemeral operations will not be supported on multi-zk mode!
 */
public final class ZKUtil {
  private static Logger logger = LoggerFactory.getLogger(ZKUtil.class);
  private static int RETRYLIMIT = 3;

  private ZKUtil() {
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param clusterName
   * @param zkAddress
   * @return
   */
  public static boolean isClusterSetup(String clusterName, String zkAddress) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    boolean result;
    try {
      result = isClusterSetup(clusterName, zkClient);
    } finally {
      zkClient.close();
    }
    return result;
  }

  public static boolean isClusterSetup(String clusterName, RealmAwareZkClient zkClient) {
    if (clusterName == null) {
      logger.info("Fail to check cluster setup : cluster name is null!");
      return false;
    }

    if (zkClient == null) {
      logger.info("Fail to check cluster setup : zookeeper client is null!");
      return false;
    }
    List<String> requiredPaths = new ArrayList<>();
    requiredPaths.add(PropertyPathBuilder.idealState(clusterName));
    requiredPaths.add(PropertyPathBuilder.clusterConfig(clusterName));
    requiredPaths.add(PropertyPathBuilder.instanceConfig(clusterName));
    requiredPaths.add(PropertyPathBuilder.resourceConfig(clusterName));
    requiredPaths.add(PropertyPathBuilder.propertyStore(clusterName));
    requiredPaths.add(PropertyPathBuilder.liveInstance(clusterName));
    requiredPaths.add(PropertyPathBuilder.instance(clusterName));
    requiredPaths.add(PropertyPathBuilder.externalView(clusterName));
    requiredPaths.add(PropertyPathBuilder.controller(clusterName));
    requiredPaths.add(PropertyPathBuilder.stateModelDef(clusterName));
    requiredPaths.add(PropertyPathBuilder.controllerMessage(clusterName));
    requiredPaths.add(PropertyPathBuilder.controllerError(clusterName));
    requiredPaths.add(PropertyPathBuilder.controllerStatusUpdate(clusterName));
    requiredPaths.add(PropertyPathBuilder.controllerHistory(clusterName));
    boolean isValid = true;

    boolean[] ret = new boolean[requiredPaths.size()];
    for (int i = 0; i < requiredPaths.size(); i++) {
      try {
        ret[i] = zkClient.exists(requiredPaths.get(i));
      } catch (Exception e) {
        ret[i] = false;
      }
    }
    StringBuilder errorMsg = new StringBuilder();

    for (int i = 0; i < ret.length; i++) {
      if (!ret[i]) {
        isValid = false;
        errorMsg.append("Invalid cluster setup for cluster: ").append(clusterName)
            .append(", missing znode path: ").append(requiredPaths.get(i)).append("\n");
      }
    }

    if (!isValid) {
      logger.warn(errorMsg.toString());
    }

    return isValid;
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param clusterName
   * @param instanceName
   * @param type
   * @return
   */
  public static boolean isInstanceSetup(String zkAddress, String clusterName, String instanceName,
      InstanceType type) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    boolean result;
    try {
      result = isInstanceSetup(zkClient, clusterName, instanceName, type);
    } finally {
      zkClient.close();
    }
    return result;
  }

  public static boolean isInstanceSetup(RealmAwareZkClient zkclient, String clusterName,
      String instanceName, InstanceType type) {
    if (type == InstanceType.PARTICIPANT || type == InstanceType.CONTROLLER_PARTICIPANT) {
      List<String> requiredPaths = new ArrayList<>();
      requiredPaths.add(PropertyPathBuilder.instanceConfig(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceMessage(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceStatusUpdate(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceError(clusterName, instanceName));
      boolean isValid = true;

      for (String path : requiredPaths) {
        if (!zkclient.exists(path)) {
          isValid = false;
          logger.info("Invalid instance setup, missing znode path: {}", path);
        }
      }

      if (isValid) {
        // Create the instance history node if it does not exist.
        // This is for back-compatibility.
        String historyPath = PropertyPathBuilder.instanceHistory(clusterName, instanceName);
        if (!zkclient.exists(historyPath)) {
          zkclient.createPersistent(historyPath, true);
        }
      }
      return isValid;
    }

    return true;
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param parentPath
   * @param list
   */
  public static void createChildren(String zkAddress, String parentPath, List<ZNRecord> list) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      createChildren(zkClient, parentPath, list);
    } finally {
      zkClient.close();
    }
  }

  public static void createChildren(RealmAwareZkClient client, String parentPath,
      List<ZNRecord> list) {
    client.createPersistent(parentPath, true);
    if (list != null) {
      for (ZNRecord record : list) {
        createChildren(client, parentPath, record);
      }
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param parentPath
   * @param nodeRecord
   */
  public static void createChildren(String zkAddress, String parentPath, ZNRecord nodeRecord) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      createChildren(zkClient, parentPath, nodeRecord);
    } finally {
      zkClient.close();
    }
  }

  public static void createChildren(RealmAwareZkClient client, String parentPath,
      ZNRecord nodeRecord) {
    client.createPersistent(parentPath, true);

    String id = nodeRecord.getId();
    String temp = parentPath + "/" + id;
    client.createPersistent(temp, nodeRecord);
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param parentPath
   * @param list
   */
  public static void dropChildren(String zkAddress, String parentPath, List<ZNRecord> list) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      dropChildren(zkClient, parentPath, list);
    } finally {
      zkClient.close();
    }
  }

  public static void dropChildren(RealmAwareZkClient client, String parentPath,
      List<ZNRecord> list) {
    // TODO: check if parentPath exists
    if (list != null) {
      for (ZNRecord record : list) {
        dropChildren(client, parentPath, record);
      }
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param parentPath
   * @param nodeRecord
   */
  public static void dropChildren(String zkAddress, String parentPath, ZNRecord nodeRecord) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      dropChildren(zkClient, parentPath, nodeRecord);
    } finally {
      zkClient.close();
    }
  }

  public static void dropChildren(RealmAwareZkClient client, String parentPath,
      ZNRecord nodeRecord) {
    // TODO: check if parentPath exists
    String id = nodeRecord.getId();
    String temp = parentPath + "/" + id;
    client.deleteRecursively(temp);
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @return
   */
  public static List<ZNRecord> getChildren(String zkAddress, String path) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    List<ZNRecord> result;
    try {
      result = getChildren(zkClient, path);
    } finally {
      zkClient.close();
    }
    return result;
  }

  public static List<ZNRecord> getChildren(RealmAwareZkClient client, String path) {
    // parent watch will be set by zkClient
    List<String> children = client.getChildren(path);
    if (children == null || children.size() == 0) {
      return Collections.emptyList();
    }

    List<ZNRecord> childRecords = new ArrayList<ZNRecord>();
    for (String child : children) {
      String childPath = path + "/" + child;
      Stat newStat = new Stat();
      ZNRecord record = client.readDataAndStat(childPath, newStat, true);
      if (record != null) {
        record.setVersion(newStat.getVersion());
        record.setCreationTime(newStat.getCtime());
        record.setModifiedTime(newStat.getMtime());
        record.setEphemeralOwner(newStat.getEphemeralOwner());
        childRecords.add(record);
      }
    }
    return childRecords;
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param record
   * @param mergeOnUpdate
   */
  public static void updateIfExists(String zkAddress, String path, final ZNRecord record,
      boolean mergeOnUpdate) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      updateIfExists(zkClient, path, record, mergeOnUpdate);
    } finally {
      zkClient.close();
    }
  }

  public static void updateIfExists(RealmAwareZkClient client, String path, final ZNRecord record,
      boolean mergeOnUpdate) {
    if (client.exists(path)) {
      DataUpdater<Object> updater = new DataUpdater<Object>() {
        @Override
        public Object update(Object currentData) {
          return record;
        }
      };
      client.updateDataSerialized(path, updater);
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param record
   * @param persistent
   * @param mergeOnUpdate
   */
  public static void createOrMerge(String zkAddress, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      createOrMerge(zkClient, path, record, persistent, mergeOnUpdate);
    } finally {
      zkClient.close();
    }
  }

  public static void createOrMerge(RealmAwareZkClient client, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT) {
      try {
        if (client.exists(path)) {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
            @Override
            public ZNRecord update(ZNRecord currentData) {
              if (currentData != null && mergeOnUpdate) {
                currentData.merge(record);
                return currentData;
              }
              return record;
            }
          };
          client.updateDataSerialized(path, updater);
        } else {
          CreateMode mode = (persistent) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
          if (record.getDeltaList().size() > 0) {
            ZNRecord value = new ZNRecord(record.getId());
            value.merge(record);
            client.create(path, value, mode);
          } else {
            client.create(path, record, mode);
          }
        }
        break;
      } catch (Exception e) {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to update " + path + " Exception:" + e.getMessage()
            + ". Will retry.");
      }
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param record
   * @param persistent
   * @param mergeOnUpdate
   */
  public static void createOrUpdate(String zkAddress, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      createOrUpdate(zkClient, path, record, persistent, mergeOnUpdate);
    } finally {
      zkClient.close();
    }
  }

  public static void createOrUpdate(RealmAwareZkClient client, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT) {
      try {
        if (client.exists(path)) {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
            @Override
            public ZNRecord update(ZNRecord currentData) {
              if (currentData != null && mergeOnUpdate) {
                currentData.update(record);
                return currentData;
              }
              return record;
            }
          };
          client.updateDataSerialized(path, updater);
        } else {
          CreateMode mode = (persistent) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
          client.create(path, record, mode);
        }
        break;
      } catch (Exception e) {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to update " + path + " Exception:" + e.getMessage()
            + ". Will retry.");
      }
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param record
   * @param persistent
   * @param mergeOnUpdate
   */
  public static void asyncCreateOrMerge(String zkAddress, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      asyncCreateOrMerge(zkClient, path, record, persistent, mergeOnUpdate);
    } finally {
      zkClient.close();
    }
  }

  public static void asyncCreateOrMerge(RealmAwareZkClient client, String path,
      final ZNRecord record, final boolean persistent, final boolean mergeOnUpdate) {
    try {
      if (client.exists(path)) {
        if (mergeOnUpdate) {
          ZNRecord curRecord = client.readData(path);
          if (curRecord != null) {
            curRecord.merge(record);
            client.asyncSetData(path, curRecord, -1, null);
          } else {
            client.asyncSetData(path, record, -1, null);
          }
        } else {
          client.asyncSetData(path, record, -1, null);
        }
      } else {
        CreateMode mode = (persistent) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
        if (record.getDeltaList().size() > 0) {
          ZNRecord newRecord = new ZNRecord(record.getId());
          newRecord.merge(record);
          client.create(path, null, mode);

          client.asyncSetData(path, newRecord, -1, null);
        } else {
          client.create(path, null, mode);

          client.asyncSetData(path, record, -1, null);
        }
      }
    } catch (Exception e) {
      logger.error("Exception in async create or update " + path + ". Exception: " + e.getMessage()
          + ". Give up.");
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param record
   * @param persistent
   */
  public static void createOrReplace(String zkAddress, String path, final ZNRecord record,
      final boolean persistent) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      createOrReplace(zkClient, path, record, persistent);
    } finally {
      zkClient.close();
    }
  }

  public static void createOrReplace(RealmAwareZkClient client, String path, final ZNRecord record,
      final boolean persistent) {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT) {
      try {
        if (client.exists(path)) {
          DataUpdater<Object> updater = new DataUpdater<Object>() {
            @Override
            public Object update(Object currentData) {
              return record;
            }
          };
          client.updateDataSerialized(path, updater);
        } else {
          CreateMode mode = (persistent) ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL;
          client.create(path, record, mode);
        }
        break;
      } catch (Exception e) {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to createOrReplace " + path + " Exception:" + e.getMessage()
            + ". Will retry.");
      }
    }
  }

  /**
   * Note: this method will create a dedicated ZkClient on the fly. Creating and closing a
   * ZkConnection is a costly operation - use it at your own risk!
   * @param zkAddress
   * @param path
   * @param recordTosubtract
   */
  public static void subtract(String zkAddress, final String path,
      final ZNRecord recordTosubtract) {
    RealmAwareZkClient zkClient = getHelixZkClient(zkAddress);
    try {
      subtract(zkClient, path, recordTosubtract);
    } finally {
      zkClient.close();
    }
  }

  public static void subtract(RealmAwareZkClient client, final String path,
      final ZNRecord recordTosubtract) {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT) {
      try {
        if (client.exists(path)) {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
            @Override
            public ZNRecord update(ZNRecord currentData) {
              if (currentData == null) {
                throw new HelixException(
                    String.format("subtract DataUpdater: ZNode at path %s is not found!", path));
              }
              currentData.subtract(recordTosubtract);
              return currentData;
            }
          };
          client.updateDataSerialized(path, updater);
          break;
        }
      } catch (Exception e) {
        retryCount = retryCount + 1;
        logger.warn("Exception trying to createOrReplace " + path + ". Will retry.", e);
      }
    }
  }

  /**
   * Converts a session id in hexadecimal notation from a long type session id.
   * Ex. 1000a5ceb930004 is returned.
   *
   * @return String representation of session id in hexadecimal notation.
   */
  public static String toHexSessionId(long sessionId) {
    return Long.toHexString(sessionId);
  }

  /**
   * Returns a dedicated ZkClient. A federatedZkClient will be used on multi-zk mode.
   * WARNING: ephemeral operations will not be supported on multi-zk mode!
   * @return
   */
  private static RealmAwareZkClient getHelixZkClient(String zkAddr) {
    if (Boolean.getBoolean(SystemPropertyKeys.MULTI_ZK_ENABLED) || zkAddr == null) {
      try {
        // Create realm-aware ZkClient.
        RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build();
        RealmAwareZkClient.RealmAwareZkClientConfig clientConfig =
            new RealmAwareZkClient.RealmAwareZkClientConfig();
        return new FederatedZkClient(connectionConfig, clientConfig);
      } catch (IllegalArgumentException | InvalidRoutingDataException e) {
        throw new HelixException("Not able to connect on realm-aware mode", e);
      }
    }
    if (zkAddr.isEmpty()) {
      throw new HelixException("ZK Address given is empty!");
    }
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());
    return DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddr), clientConfig);
  }

  /**
   * Convert Stat fields into a Map.
   *     private long czxid;
   *     private long mzxid;
   *     private long ctime;
   *     private long mtime;
   *     private int version;
   *     private int cversion;
   *     private int aversion;
   *     private long ephemeralOwner;
   *     private int dataLength;
   *     private int numChildren;
   *     private long pzxid;
   * @param stat
   * @return
   */
  public static Map<String, String> fromStatToMap(Stat stat) {
    if (stat == null) {
      throw new HelixException("Stat cannot be null!");
    }
    Map<String, String> statMap = new HashMap<>();
    statMap.put("czxid", Long.toString(stat.getCzxid()));
    statMap.put("mzxid", Long.toString(stat.getMzxid()));
    statMap.put("ctime", Long.toString(stat.getCtime()));
    statMap.put("mtime", Long.toString(stat.getMtime()));
    statMap.put("version", Integer.toString(stat.getVersion()));
    statMap.put("cversion", Integer.toString(stat.getCversion()));
    statMap.put("aversion", Integer.toString(stat.getAversion()));
    statMap.put("ephemeralOwner", Long.toString(stat.getEphemeralOwner()));
    statMap.put("dataLength", Integer.toString(stat.getDataLength()));
    statMap.put("numChildren", Integer.toString(stat.getNumChildren()));
    statMap.put("pzxid", Long.toString(stat.getPzxid()));
    return statMap;
  }
}
