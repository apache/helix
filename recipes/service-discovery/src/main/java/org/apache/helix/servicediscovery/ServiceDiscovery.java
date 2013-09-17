package org.apache.helix.servicediscovery;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;

public class ServiceDiscovery {
  private final String zkAddress;
  private final String cluster;
  private HelixManager admin;
  List<ServiceMetadata> cache;
  static int DEFAULT_POLL_INTERVAL = 30 * 1000; // in ms
  private final Mode mode;
  Map<String, HelixManager> serviceMap;
  private Timer timer;

  enum Mode {
    NONE, // No monitoring, only registration, on demand reading zk
    WATCH, // Watches ZK path
    POLL// Polls zk at some intervals
  }

  public ServiceDiscovery(String zkAddress, String cluster, Mode mode) {
    this.zkAddress = zkAddress;
    this.cluster = cluster;
    this.mode = mode;
    serviceMap = new HashMap<String, HelixManager>();
    cache = Collections.emptyList();
  }

  /**
   * @return returns true if
   */
  public boolean start() throws Exception {
    // auto create cluster and allow nodes to automatically join the cluster
    admin =
        HelixManagerFactory.getZKHelixManager(cluster, "service-discovery",
            InstanceType.ADMINISTRATOR, zkAddress);
    admin.connect();
    admin.getClusterManagmentTool().addCluster(cluster, false);
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(cluster).build();

    Map<String, String> properties = new HashMap<String, String>();
    properties.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
    admin.getClusterManagmentTool().setConfig(scope, properties);
    switch (mode) {
    case POLL:
      startBackgroundTask();
      break;
    case WATCH:
      setupWatcher();
      break;
    case NONE:// dont monitor changes, supports only registration

    }
    refreshCache();
    return true;
  }

  private void startBackgroundTask() {
    TimerTask timertask = new TimerTask() {

      @Override
      public void run() {
        refreshCache();
      }
    };
    timer = new Timer();
    timer.scheduleAtFixedRate(timertask, DEFAULT_POLL_INTERVAL, DEFAULT_POLL_INTERVAL);
  }

  private void setupWatcher() throws Exception {

    LiveInstanceChangeListener listener = new LiveInstanceChangeListener() {
      @Override
      public void onLiveInstanceChange(List<LiveInstance> liveInstances,
          NotificationContext changeContext) {
        if (changeContext.getType() != NotificationContext.Type.FINALIZE) {
          refreshCache();
        }
      }
    };
    admin.addLiveInstanceChangeListener(listener);
  }

  public boolean stop() {
    if (admin != null && admin.isConnected()) {
      admin.disconnect();
    }
    if (timer != null) {
      timer.cancel();
    }
    return true;
  }

  public void deregister(final String serviceId) {
    HelixManager helixManager = serviceMap.get(serviceId);
    if (helixManager != null && helixManager.isConnected()) {
      helixManager.disconnect();
    }
  }

  public boolean register(final String serviceId, final ServiceMetadata serviceMetadata)
      throws Exception {
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(cluster, serviceId, InstanceType.PARTICIPANT,
            zkAddress);
    LiveInstanceInfoProvider liveInstanceInfoProvider = new LiveInstanceInfoProvider() {
      @Override
      public ZNRecord getAdditionalLiveInstanceInfo() {
        // serialize serviceMetadata to ZNRecord
        ZNRecord rec = new ZNRecord(serviceId);
        rec.setSimpleField("HOST", serviceMetadata.getHost());
        rec.setSimpleField("PORT", String.valueOf(serviceMetadata.getPort()));
        rec.setSimpleField("SERVICE_NAME", serviceMetadata.getServiceName());
        return rec;
      }
    };
    helixManager.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
    helixManager.connect();
    serviceMap.put(serviceId, helixManager);
    refreshCache();
    return true;
  }

  private void refreshCache() {
    Builder propertyKeyBuilder = new PropertyKey.Builder(cluster);
    HelixDataAccessor helixDataAccessor = admin.getHelixDataAccessor();
    List<LiveInstance> liveInstances =
        helixDataAccessor.getChildValues(propertyKeyBuilder.liveInstances());
    refreshCache(liveInstances);
  }

  private void refreshCache(List<LiveInstance> liveInstances) {
    List<ServiceMetadata> services = new ArrayList<ServiceMetadata>();
    for (LiveInstance liveInstance : liveInstances) {
      ServiceMetadata metadata = new ServiceMetadata();
      ZNRecord rec = liveInstance.getRecord();
      metadata.setPort(Integer.parseInt(rec.getSimpleField("PORT")));
      metadata.setHost(rec.getSimpleField("HOST"));
      metadata.setServiceName(rec.getSimpleField("SERVICE_NAME"));
      services.add(metadata);
    }
    // protect against multiple threads updating this
    synchronized (this) {
      cache = services;
    }
  }

  public List<ServiceMetadata> findAllServices() {
    if (mode == Mode.NONE) {
      refreshCache();
    }
    return cache;
  }
}
