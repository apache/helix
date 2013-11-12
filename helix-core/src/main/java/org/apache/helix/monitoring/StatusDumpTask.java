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

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.store.ZNRecordJsonSerializer;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class StatusDumpTask extends HelixTimerTask {
  final static Logger LOG = Logger.getLogger(StatusDumpTask.class);

  Timer _timer = null;
  final HelixDataAccessor _accessor;
  final ClusterId _clusterId;

  class StatusDumpTimerTask extends TimerTask {
    final HelixDataAccessor _accessor;
    final PropertyKey.Builder _keyBuilder;
    final BaseDataAccessor<ZNRecord> _baseAccessor;
    final ZNRecordJsonSerializer _serializer;
    final long _thresholdNoChangeInMs;
    final ClusterId _clusterId;

    public StatusDumpTimerTask(ClusterId clusterId, HelixDataAccessor accessor,
        long thresholdNoChangeInMs) {
      _accessor = accessor;
      _keyBuilder = accessor.keyBuilder();
      _baseAccessor = accessor.getBaseDataAccessor();
      _serializer = new ZNRecordJsonSerializer();
      _thresholdNoChangeInMs = thresholdNoChangeInMs;
      _clusterId = clusterId;
    }

    @Override
    public void run() {
      /**
       * For each record in status-update and error znode
       * TODO: for now the status updates are dumped to cluster controller's log.
       * We need to think if we should create per-instance log files that contains
       * per-instance status-updates and errors
       */
      LOG.info("Scannning status updates ...");
      try {
        List<String> instanceNames = _accessor.getChildNames(_keyBuilder.instanceConfigs());
        for (String instanceName : instanceNames) {

          scanPath(_keyBuilder.statusUpdates(instanceName).getPath());
          scanPath(HelixUtil.getInstancePropertyPath(_clusterId.stringify(), instanceName,
              PropertyType.ERRORS));
        }

        scanPath(HelixUtil.getControllerPropertyPath(_clusterId.stringify(),
            PropertyType.STATUSUPDATES_CONTROLLER));
        scanPath(HelixUtil.getControllerPropertyPath(_clusterId.stringify(),
            PropertyType.ERRORS_CONTROLLER));
      } catch (Exception e) {
        LOG.error("Exception dumping status/errors, clusterId: " + _clusterId, e);
      }
    }

    // TODO: refactor this
    void scanPath(String path) {
      LOG.info("Scannning path: " + path);
      List<String> childs = _baseAccessor.getChildNames(path, 0);
      if (childs == null || childs.isEmpty()) {
        return;
      }

      for (String child : childs) {
        String childPath = path + "/" + child;

        try {
          List<String> grandChilds = _baseAccessor.getChildNames(childPath, 0);
          if (grandChilds == null || grandChilds.isEmpty()) {
            continue;
          }

          for (String grandChild : grandChilds) {
            String grandChildPath = childPath + "/" + grandChild;
            try {
              checkAndDump(grandChildPath);
            } catch (Exception e) {
              LOG.error("Exception in dumping status, path: " + grandChildPath, e);
            }
          }
        } catch (Exception e) {
          LOG.error("Exception in dumping status, path: " + childPath, e);
        }
      }
    }

    void checkAndDump(String path) {
      List<String> paths = new ArrayList<String>();
      paths.add(path);

      List<String> childs = _baseAccessor.getChildNames(path, 0);
      if (childs != null && !childs.isEmpty()) {
        for (String child : childs) {
          String childPath = path + "/" + child;
          paths.add(childPath);
        }
      }

      long nowInMs = System.currentTimeMillis();

      List<Stat> stats = new ArrayList<Stat>();
      List<ZNRecord> records = _baseAccessor.get(paths, stats, 0);
      for (int i = 0; i < paths.size(); i++) {
        String dumpPath = paths.get(i);
        Stat stat = stats.get(i);
        ZNRecord record = records.get(i);
        long timePassedInMs = nowInMs - stat.getMtime();
        if (timePassedInMs > _thresholdNoChangeInMs) {
          LOG.info("Dumping status update path: " + dumpPath + ", " + timePassedInMs
              + "MS has passed");
          try {
            LOG.info(new String(_serializer.serialize(record)));
          } catch (Exception e) {
            LOG.warn("Ignorable exception serializing path: " + dumpPath + ", record: " + record, e);
          }
          _baseAccessor.remove(dumpPath, 0);
        }
      }
    }
  }

  public StatusDumpTask(ClusterId clusterId, HelixDataAccessor accessor) {
    _accessor = accessor;
    _clusterId = clusterId;
  }

  @Override
  public void start() {
    final long initialDelay = 30 * 60 * 1000;
    final long period = 120 * 60 * 1000;
    final long thresholdNoChangeInMs = 180 * 60 * 1000;

    if (_timer == null) {
      LOG.info("Start StatusDumpTask");
      _timer = new Timer("StatusDumpTimerTask", true);
      _timer.scheduleAtFixedRate(new StatusDumpTimerTask(_clusterId, _accessor,
          thresholdNoChangeInMs), initialDelay, period);
    }

  }

  @Override
  public void stop() {
    if (_timer != null) {
      LOG.info("Stop StatusDumpTask");
      _timer.cancel();
      _timer = null;
    }
  }
}
