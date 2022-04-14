package org.apache.helix.rest.clusterMaintenanceService;

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

import java.util.ArrayList;
import java.util.List;


public class MaintenanceManagementInstanceInfo {

  public enum OperationalStatus {
    SUCCESS,
    FAILURE
  }

  private String _operationResult;
  private OperationalStatus _status;
  private List<String> _messages;

  public MaintenanceManagementInstanceInfo(OperationalStatus status) {
    this._status = status;
    this._messages = new ArrayList<>();
    this._operationResult = "";
  }

  public MaintenanceManagementInstanceInfo(OperationalStatus status, List<String> messages) {
    this._status = status;
    this._messages = messages;
    this._operationResult = "";
  }

  public MaintenanceManagementInstanceInfo(OperationalStatus status, String newOperationResult) {
    this._status = status;
    this._operationResult = newOperationResult;
    this._messages = new ArrayList<>();
  }

  public List<String> getMessages() {
    return _messages;
  }

  public String getOperationResult() {
    return _operationResult;
  }

  public boolean hasOperationResult() {
    return !_operationResult.isEmpty();
  }

  public void setOperationResult(String result) {
    _operationResult = result;
  }

  public void addMessages(List<String> msg) {
    _messages.addAll(msg);
  }

  public void addMessage(String meg) {
    _messages.add(meg);
  }

  public boolean isSuccessful() {
    return _status.equals(OperationalStatus.SUCCESS);
  }

  public void mergeResult(MaintenanceManagementInstanceInfo info) {
    mergeResult(info, false);
  }

  public void mergeResult(MaintenanceManagementInstanceInfo info, boolean nonBlockingFailure) {
    if (info == null) {
      return;
    }
    _messages.addAll(info.getMessages());
    _status =
        ((info.isSuccessful() || nonBlockingFailure) && isSuccessful()) ? OperationalStatus.SUCCESS
            : OperationalStatus.FAILURE;
    if (info.hasOperationResult()) {
      _operationResult =
          this.hasOperationResult() ? _operationResult + "," + info.getOperationResult()
              : info.getOperationResult();
    }
  }
}