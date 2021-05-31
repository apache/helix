package org.apache.helix.controller.dataproviders;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.PauseSignal;

/**
 * Data provider for controller management mode pipeline.
 */
public class ManagementControllerDataProvider extends BaseControllerDataProvider {
  private static final List<HelixConstants.ChangeType> FULL_REFRESH_PROPERTIES =
      Arrays.asList(HelixConstants.ChangeType.LIVE_INSTANCE, HelixConstants.ChangeType.MESSAGE);

  private PauseSignal _pauseSignal;
  private boolean _isControllerPaused;
  // Whether any enabled live instance is still in abnormal status, eg. frozen status.
  private boolean _hasAbnormalEnabledLiveInstance;

  // cache the previous pause/maintenance status for triggering resume event.
  private boolean _prevInMaintenanceMode;
  private boolean _prevShouldRunManagementPipeline;

  public ManagementControllerDataProvider(String clusterName, String pipelineName) {
    super(clusterName, pipelineName);
  }

  @Override
  public void requireFullRefresh() {
    for (HelixConstants.ChangeType type : FULL_REFRESH_PROPERTIES) {
      _propertyDataChangedMap.get(type).set(true);
    }
  }

  @Override
  public void refresh(HelixDataAccessor accessor) {
    _prevInMaintenanceMode = isMaintenanceModeEnabled();
    _prevShouldRunManagementPipeline = shouldRunManagementPipeline();

    super.refresh(accessor);
    refreshPauseSignal(accessor);

    // Check if there is any enabled live instance not in NORMAL status
    checkAbnormalEnabledLiveInstance();
  }

  /**
   * Whether the pipeline should run. If the management mode pipeline is running,
   * the resource/task pipelines should not be run. Vice versa.
   *
   * @return true if the pipeline should run; otherwise, false
   */
  public boolean shouldRunManagementPipeline() {
    return _isControllerPaused || _hasAbnormalEnabledLiveInstance;
  }

  /**
   * WARNING: the logic here is tricky.
   * 1. Only resume if not paused. So if the Maintenance mode is removed but the cluster is still
   * paused, the resume event should not be sent.
   * 2. Only send resume event if the status is changed back to active. So we don't send multiple
   * event unnecessarily.
   */
  public boolean shouldTriggerResume() {
    return !shouldRunManagementPipeline() && (_prevShouldRunManagementPipeline || (
        _prevInMaintenanceMode && !isMaintenanceModeEnabled()));
  }

  private void refreshPauseSignal(HelixDataAccessor accessor) {
    _pauseSignal = accessor.getProperty(accessor.keyBuilder().pause());
    _isControllerPaused = (_pauseSignal != null);
  }

  private void checkAbnormalEnabledLiveInstance() {
    Map<String, LiveInstance> liveInstanceMap = getLiveInstances();
    _hasAbnormalEnabledLiveInstance = getEnabledLiveInstances().stream()
        .noneMatch(instance -> liveInstanceMap.get(instance).getStatus() != null);
  }
}
