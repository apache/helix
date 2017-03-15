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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public final class ZKUtil {
  private static Logger logger = Logger.getLogger(ZKUtil.class);
  private static int RETRYLIMIT = 3;

  private ZKUtil() {
  }

  public static boolean isClusterSetup(String clusterName, ZkClient zkClient) {
    if (clusterName == null) {
      logger.info("Fail to check cluster setup : cluster name is null!");
      return false;
    }

    if (zkClient == null) {
      logger.info("Fail to check cluster setup : zookeeper client is null!");
      return false;
    }
    ArrayList<String> requiredPaths = new ArrayList<String>();
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

    BaseDataAccessor<Object> baseAccessor = new ZkBaseDataAccessor<Object>(zkClient);
    boolean[] ret = baseAccessor.exists(requiredPaths, 0);
    StringBuilder errorMsg = new StringBuilder();

    for (int i = 0; i < ret.length; i++) {
      if (!ret[i]) {
        isValid = false;
        errorMsg
            .append(("Invalid cluster setup, missing znode path: " + requiredPaths.get(i)) + "\n");
      }
    }

    if (!isValid) {
      logger.debug(errorMsg.toString());
    }

    return isValid;
  }

  public static boolean isInstanceSetup(ZkClient zkclient, String clusterName, String instanceName,
      InstanceType type) {
    if (type == InstanceType.PARTICIPANT || type == InstanceType.CONTROLLER_PARTICIPANT) {
      ArrayList<String> requiredPaths = new ArrayList<String>();
      requiredPaths.add(PropertyPathBuilder.instanceConfig(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceMessage(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceCurrentState(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceStatusUpdate(clusterName, instanceName));
      requiredPaths.add(PropertyPathBuilder.instanceError(clusterName, instanceName));
      boolean isValid = true;

      for (String path : requiredPaths) {
        if (!zkclient.exists(path)) {
          isValid = false;
          logger.info("Invalid instance setup, missing znode path: " + path);
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

  public static void createChildren(ZkClient client, String parentPath, List<ZNRecord> list) {
    client.createPersistent(parentPath, true);
    if (list != null) {
      for (ZNRecord record : list) {
        createChildren(client, parentPath, record);
      }
    }
  }

  public static void createChildren(ZkClient client, String parentPath, ZNRecord nodeRecord) {
    client.createPersistent(parentPath, true);

    String id = nodeRecord.getId();
    String temp = parentPath + "/" + id;
    client.createPersistent(temp, nodeRecord);
  }

  public static void dropChildren(ZkClient client, String parentPath, List<ZNRecord> list) {
    // TODO: check if parentPath exists
    if (list != null) {
      for (ZNRecord record : list) {
        dropChildren(client, parentPath, record);
      }
    }
  }

  public static void dropChildren(ZkClient client, String parentPath, ZNRecord nodeRecord) {
    // TODO: check if parentPath exists
    String id = nodeRecord.getId();
    String temp = parentPath + "/" + id;
    client.deleteRecursive(temp);
  }

  public static List<ZNRecord> getChildren(ZkClient client, String path) {
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
        childRecords.add(record);
      }
    }
    return childRecords;
  }

  public static void updateIfExists(ZkClient client, String path, final ZNRecord record,
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

  public static void createOrUpdate(ZkClient client, String path, final ZNRecord record,
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

  public static void asyncCreateOrUpdate(ZkClient client, String path, final ZNRecord record,
      final boolean persistent, final boolean mergeOnUpdate) {
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

  public static void createOrReplace(ZkClient client, String path, final ZNRecord record,
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

  public static void subtract(ZkClient client, String path, final ZNRecord recordTosubtract) {
    int retryCount = 0;
    while (retryCount < RETRYLIMIT) {
      try {
        if (client.exists(path)) {
          DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>() {
            @Override
            public ZNRecord update(ZNRecord currentData) {
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
}
