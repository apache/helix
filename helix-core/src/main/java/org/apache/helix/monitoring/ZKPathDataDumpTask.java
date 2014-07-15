package org.apache.helix.monitoring;

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

import java.util.List;
import java.util.TimerTask;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.Lists;

public class ZKPathDataDumpTask extends TimerTask {
  static Logger LOG = Logger.getLogger(ZKPathDataDumpTask.class);

  private final long _thresholdNoChangeMsForStatusUpdates;
  private final long _thresholdNoChangeMsForErrors;
  private final int _maxLeafCount;
  private final HelixManager _manager;
  private final ZNRecordSerializer _jsonSerializer;

  public ZKPathDataDumpTask(HelixManager manager, long thresholdNoChangeMsForStatusUpdates,
      long thresholdNoChangeMsForErrors, int maxLeafCount) {
    LOG.info("Init ZKPathDataDumpTask for cluster: " + manager.getClusterName()
        + ", thresholdNoChangeMsForStatusUpdates: " + thresholdNoChangeMsForStatusUpdates
        + ", thresholdNoChangeMsForErrors: " + thresholdNoChangeMsForErrors + ", maxLeafCount: "
        + maxLeafCount);

    _manager = manager;
    _jsonSerializer = new ZNRecordSerializer();
    _thresholdNoChangeMsForStatusUpdates = thresholdNoChangeMsForStatusUpdates;
    _thresholdNoChangeMsForErrors = thresholdNoChangeMsForErrors;
    _maxLeafCount = maxLeafCount;
  }

  @Override
  public void run() {
    // For each record in status update and error node
    // TODO: for now the status updates are dumped to cluster manager log4j log.
    // We need to think if we should create per-instance log files that contains
    // per-instance statusUpdates
    // and errors
    LOG.info("Scan statusUpdates and errors for cluster: " + _manager.getClusterName()
        + ", by controller: " + _manager);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    BaseDataAccessor<ZNRecord> baseAccessor = accessor.getBaseDataAccessor();

    List<String> instances = accessor.getChildNames(keyBuilder.instanceConfigs());
    for (String instance : instances) {
      // dump participant status updates
      String statusUpdatePath =
          HelixUtil.getInstancePropertyPath(_manager.getClusterName(), instance,
              PropertyType.STATUSUPDATES);
      dump(baseAccessor, statusUpdatePath, _thresholdNoChangeMsForStatusUpdates, _maxLeafCount);

      // dump participant errors
      String errorPath =
          HelixUtil.getInstancePropertyPath(_manager.getClusterName(), instance,
              PropertyType.ERRORS);
      dump(baseAccessor, errorPath, _thresholdNoChangeMsForErrors, _maxLeafCount);
    }
    // dump controller status updates
    String controllerStatusUpdatePath =
        HelixUtil.getControllerPropertyPath(_manager.getClusterName(),
            PropertyType.STATUSUPDATES_CONTROLLER);
    dump(baseAccessor, controllerStatusUpdatePath, _thresholdNoChangeMsForStatusUpdates,
        _maxLeafCount);

    // dump controller errors
    String controllerErrorPath =
        HelixUtil.getControllerPropertyPath(_manager.getClusterName(),
            PropertyType.ERRORS_CONTROLLER);
    dump(baseAccessor, controllerErrorPath, _thresholdNoChangeMsForErrors, _maxLeafCount);
  }

  /**
   * Find paths of all leaf nodes under an ancestor path (exclusive)
   * @param accessor
   * @param ancestorPath
   * @return a list of paths
   */
  static List<String> scanPath(BaseDataAccessor<ZNRecord> accessor, String ancestorPath) {
    List<String> queue = Lists.newLinkedList();
    queue.add(ancestorPath);

    // BFS
    List<String> leafPaths = Lists.newArrayList();
    while (!queue.isEmpty()) {
      String path = queue.remove(0);
      List<String> childNames = accessor.getChildNames(path, 0);
      if (childNames == null) {
        // path doesn't exist
        continue;
      }
      if (childNames.isEmpty() && !path.equals(ancestorPath)) {
        // leaf node, excluding ancestorPath
        leafPaths.add(path);
      }
      for (String childName : childNames) {
        String subPath = String.format("%s/%s", path, childName);
        queue.add(subPath);
      }
    }
    return leafPaths;
  }

  void dump(BaseDataAccessor<ZNRecord> accessor, String ancestorPath, long threshold,
      int maxLeafCount) {
    List<String> leafPaths = scanPath(accessor, ancestorPath);
    if (leafPaths.isEmpty()) {
      return;
    }

    Stat[] stats = accessor.getStats(leafPaths, 0);
    List<String> dumpPaths = Lists.newArrayList();
    long now = System.currentTimeMillis();
    for (int i = 0; i < stats.length; i++) {
      Stat stat = stats[i];
      if ((stats.length > maxLeafCount) || ((now - stat.getMtime()) > threshold)) {
        dumpPaths.add(leafPaths.get(i));
      }
    }

    // dump
    LOG.info("Dump statusUpdates and errors records for pahts: " + dumpPaths);
    List<ZNRecord> dumpRecords = accessor.get(dumpPaths, null, 0);
    for (ZNRecord record : dumpRecords) {
      if (record != null) {
        LOG.info(new String(_jsonSerializer.serialize(record)));
      }
    }

    // clean up
    accessor.remove(dumpPaths, 0);
  }
}
