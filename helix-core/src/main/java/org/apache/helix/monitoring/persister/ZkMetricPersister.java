package org.apache.helix.monitoring.persister;

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

import java.util.Map;
import java.util.Set;

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.monitoring.mbeans.JobMonitor;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkMetricPersister implements MetricPersister {
  private final BaseDataAccessor<ZNRecord> _baseDataAccessor;

  private static final Logger LOG = LoggerFactory.getLogger(JobMonitor.class);
  private static final String PERSISTED_METRICS_ZK_PATH = "/PERSISTED_METRICS";

  public enum ZkMetricPersisterMetadataKey {
    ZNODE_NAME,
    CLUSTER_NAME
  }

  public ZkMetricPersister(HelixManager helixManager) {
    if (helixManager == null || !helixManager.isConnected()) {
      throw new HelixException(
          "Helix Manager is null or is not connected, cannot create ZkMetricPersister!");
    }

    _baseDataAccessor = helixManager.getHelixDataAccessor().getBaseDataAccessor();
  }

  public void persistMetricsMeanValues(Set<HistogramDynamicMetric> metrics,
      Map<String, String> metadata) {
    String znodeName = metadata.get(ZkMetricPersisterMetadataKey.ZNODE_NAME.name());
    String zkPath =
        buildMetricPersistZkPath(metadata.get(ZkMetricPersisterMetadataKey.CLUSTER_NAME.name()),
            znodeName);

    ZNRecord persistedMetrics = new ZNRecord(znodeName);
    for (HistogramDynamicMetric metric : metrics) {
      persistedMetrics.setSimpleField(metric.getMetricName(), metric.getMeanValue().toString());
    }
    if (!_baseDataAccessor.set(zkPath, persistedMetrics, AccessOption.PERSISTENT)) {
      LOG.error("Persist metrics to ZK failed. Zk path: {}", zkPath);
    }
  }

  /**
   * Compute the zk path for job monitor metrics syncing.
   * @param jobType - the job type of the metric that needs to be synced
   * @return the zk path towards the ZNode that represents this job type. Since metrics are
   * aggregated by job types, all metric values of one job type should go into one ZNode.
   */
  public static String buildMetricPersistZkPath(String clusterName, String znodeName) {
    return String.format("%s/%s/%s", clusterName, PERSISTED_METRICS_ZK_PATH, znodeName);
  }
}
