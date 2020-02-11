package org.apache.helix.healthcheck;

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

import java.util.LinkedList;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.model.HealthStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParticipantHealthReportCollectorImpl implements ParticipantHealthReportCollector {
  private final LinkedList<HealthReportProvider> _healthReportProviderList =
      new LinkedList<HealthReportProvider>();
  private static final Logger LOG = LoggerFactory
      .getLogger(ParticipantHealthReportCollectorImpl.class);
  private final HelixManager _helixManager;
  String _instanceName;

  public ParticipantHealthReportCollectorImpl(HelixManager helixManager, String instanceName) {
    _helixManager = helixManager;
    _instanceName = instanceName;
  }

  @Override
  public void addHealthReportProvider(HealthReportProvider provider) {
    try {
      synchronized (_healthReportProviderList) {
        if (!_healthReportProviderList.contains(provider)) {
          _healthReportProviderList.add(provider);
        } else {
          LOG.warn("Skipping a duplicated HealthCheckInfoProvider");
        }
      }
    } catch (Exception e) {
      LOG.error(e.toString());
    }
  }

  @Override
  public void removeHealthReportProvider(HealthReportProvider provider) {
    synchronized (_healthReportProviderList) {
      if (_healthReportProviderList.contains(provider)) {
        _healthReportProviderList.remove(provider);
      } else {
        LOG.warn("Skip removing a non-exist HealthCheckInfoProvider");
      }
    }
  }

  @Override
  public void reportHealthReportMessage(ZNRecord healthCheckInfoUpdate) {
    HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
    Builder keyBuilder = accessor.keyBuilder();
    if(!accessor.setProperty(keyBuilder.healthReport(_instanceName, healthCheckInfoUpdate.getId()),
        new HealthStat(healthCheckInfoUpdate))) {
      LOG.error("Failed to persist health report to zk!");
    }
  }

  @Override
  public synchronized void transmitHealthReports() {
    synchronized (_healthReportProviderList) {
      for (HealthReportProvider provider : _healthReportProviderList) {
        try {
          Map<String, String> report = provider.getRecentHealthReport();
          Map<String, Map<String, String>> partitionReport =
              provider.getRecentPartitionHealthReport();
          ZNRecord record = new ZNRecord(provider.getReportName());
          if (report != null) {
            record.setSimpleFields(report);
          }
          if (partitionReport != null) {
            record.setMapFields(partitionReport);
          }
          record.setSimpleField(HealthStat.TIMESTAMP_NAME, "" + System.currentTimeMillis());

          HelixDataAccessor accessor = _helixManager.getHelixDataAccessor();
          Builder keyBuilder = accessor.keyBuilder();
          if (!accessor.setProperty(keyBuilder.healthReport(_instanceName, record.getId()),
              new HealthStat(record))) {
            LOG.error("Failed to persist health report to zk!");
          }

          provider.resetStats();
        } catch (Exception e) {
          LOG.error("fail to transmit health report", e);
        }
      }
    }
  }
}
