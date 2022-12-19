package org.apache.helix.cloud.event.helix;

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

import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.util.ConfigStringUtil;
import org.apache.helix.util.InstanceValidationUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class HelixEventHandlingUtil {
  private static Logger LOG = LoggerFactory.getLogger(HelixEventHandlingUtil.class);

  /**
   * check if instance is disabled by cloud event.
   * @param instanceName
   * @param dataAccessor
   * @return return true only when instance is Helix disabled and the disabled reason in
   * instanceConfig is cloudEvent
   */
  static boolean isInstanceDisabledForCloudEvent(String instanceName,
      HelixDataAccessor dataAccessor) {
    InstanceConfig instanceConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().instanceConfig(instanceName));
    if (instanceConfig == null) {
      throw new HelixException("Instance: " + instanceName
          + ", instance config does not exist");
    }
    return !InstanceValidationUtil.isEnabled(dataAccessor, instanceName) && instanceConfig
        .getInstanceDisabledType()
        .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name());
  }

  /**
   * Update map field disabledInstancesWithInfo in clusterConfig with cloudEvent instance info
   */
  static void updateCloudEventOperationInClusterConfig(String clusterName, String instanceName,
      BaseDataAccessor baseAccessor, boolean enable, String message) {
    String path = PropertyPathBuilder.clusterConfig(clusterName);

    if (!baseAccessor.exists(path, 0)) {
      throw new HelixException("Cluster " + clusterName + ": cluster config does not exist");
    }

    if (!baseAccessor.update(path, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord currentData) {
        if (currentData == null) {
          throw new HelixException("Cluster: " + clusterName + ": cluster config is null");
        }

        ClusterConfig clusterConfig = new ClusterConfig(currentData);
        Map<String, String> disabledInstancesWithInfo =
            new TreeMap<>(clusterConfig.getDisabledInstancesWithInfo());
        if (enable) {
          disabledInstancesWithInfo.keySet().remove(instanceName);
        } else {
          // disabledInstancesWithInfo is only used for cloud event handling.
          String timeStamp = String.valueOf(System.currentTimeMillis());
          disabledInstancesWithInfo.put(instanceName, ZKHelixAdmin
              .assembleInstanceBatchedDisabledInfo(
                  InstanceConstants.InstanceDisabledType.CLOUD_EVENT, message, timeStamp));
        }
        clusterConfig.setDisabledInstancesWithInfo(disabledInstancesWithInfo);

        return clusterConfig.getRecord();
      }
    }, AccessOption.PERSISTENT)) {
      LOG.error("Failed to update cluster config {} for {} instance {}. {}", clusterName,
          enable ? "enable" : "disable", instanceName, message);
    }
  }

  /**
   * Return true if no instance is under cloud event handling
   * @param clusterConfig
   * @return
   */
  static boolean checkNoInstanceUnderCloudEvent(ClusterConfig clusterConfig) {
    Map<String, String> clusterConfigTrackedEvent = clusterConfig.getDisabledInstancesWithInfo();
    if (clusterConfigTrackedEvent == null || clusterConfigTrackedEvent.isEmpty()) {
      return true;
    }

    for (Map.Entry<String, String> entry : clusterConfigTrackedEvent.entrySet()) {
      if (ConfigStringUtil.parseConcatenatedConfig(entry.getValue())
          .get(ClusterConfig.ClusterConfigProperty.HELIX_DISABLED_TYPE.toString())
          .equals(InstanceConstants.InstanceDisabledType.CLOUD_EVENT.name())) {
        return false;
      }
    }
    return true;
  }
}
