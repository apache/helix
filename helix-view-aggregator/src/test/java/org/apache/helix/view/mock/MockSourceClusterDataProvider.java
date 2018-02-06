package org.apache.helix.view.mock;

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
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.common.ClusterEventProcessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.view.dataprovider.SourceClusterDataProvider;

public class MockSourceClusterDataProvider extends SourceClusterDataProvider {

  public MockSourceClusterDataProvider(ViewClusterSourceConfig config,
      ClusterEventProcessor processor) {
    super(config, processor);
  }

  @Override
  public void setup() {}

  @Override
  public void refreshCache() {}

  @Override
  public List<String> getInstanceConfigNames() {
    return new ArrayList<>(getInstanceConfigMap().keySet());
  }

  @Override
  public List<String> getLiveInstanceNames() {
    return new ArrayList<>(getLiveInstances().keySet());
  }

  @Override
  public List<String> getExternalViewNames() {
    return new ArrayList<>(getExternalViews().keySet());
  }

  public void setConfig(ViewClusterSourceConfig config) {
    _sourceClusterConfig = config;
  }

  public ViewClusterSourceConfig getConfig() {
    return _sourceClusterConfig;
  }

  public void setInstanceConfigs(List<InstanceConfig> instanceConfigList) {
    for (InstanceConfig config : instanceConfigList) {
      _instanceConfigMap.put(config.getInstanceName(), config);
    }
  }

  public void setLiveInstances(List<LiveInstance> liveInstanceList) {
    for (LiveInstance instance : liveInstanceList) {
      _liveInstanceMap.put(instance.getInstanceName(), instance);
    }
  }

  public void setExternalViews(List<ExternalView> externalViewList) {
    for (ExternalView ev : externalViewList) {
      _externalViewMap.put(ev.getResourceName(), ev);
    }
  }
}
