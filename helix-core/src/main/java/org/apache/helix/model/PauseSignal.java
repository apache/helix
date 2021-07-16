package org.apache.helix.model;

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

import java.time.Instant;

import org.apache.helix.HelixProperty;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Represent a pause in the cluster
 */
public class PauseSignal extends HelixProperty {
  private static final String DEFAULT_PAUSE_ID = "pause";

  public enum PauseSignalProperty {
    REASON,
    CLUSTER_FREEZE,
    FROM_HOST,
    CANCEL_PENDING_ST,
    TRIGGER_TIME
  }

  public PauseSignal() {
    this(DEFAULT_PAUSE_ID);
  }

  /**
   * Instantiate with an identifier
   * @param id pause signal identifier
   */
  public PauseSignal(String id) {
    super(id);
  }

  /**
   * Instantiate with a pre-populated record
   * @param record ZNRecord with fields corresponding to a pause
   */
  public PauseSignal(ZNRecord record) {
    super(record);
  }

  /**
   * Set the reason why the cluster is paused.
   * @param reason
   */
  public void setReason(String reason) {
    _record.setSimpleField(PauseSignalProperty.REASON.name(), reason);
  }

  public String getReason() {
    return _record.getSimpleField(PauseSignalProperty.REASON.name());
  }

  @Override
  public boolean isValid() {
    return true;
  }

  public void setClusterPause(boolean pause) {
    _record.setBooleanField(PauseSignalProperty.CLUSTER_FREEZE.name(), pause);
  }

  public boolean isClusterPause() {
    return _record.getBooleanField(PauseSignalProperty.CLUSTER_FREEZE.name(), false);
  }

  public void setFromHost(String host) {
    _record.setSimpleField(PauseSignalProperty.FROM_HOST.name(), host);
  }

  public String getFromHost() {
    return _record.getSimpleField(PauseSignalProperty.FROM_HOST.name());
  }

  public void setCancelPendingST(boolean cancel) {
    _record.setBooleanField(PauseSignalProperty.CANCEL_PENDING_ST.name(), cancel);
  }

  public boolean getCancelPendingST() {
    return _record.getBooleanField(PauseSignalProperty.CANCEL_PENDING_ST.name(), false);
  }

  public void setTriggerTime(long time) {
    _record.setSimpleField(PauseSignalProperty.TRIGGER_TIME.name(),
        Instant.ofEpochMilli(time).toString());
  }
}
