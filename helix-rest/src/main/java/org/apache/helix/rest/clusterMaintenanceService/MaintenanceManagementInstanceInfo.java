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

  private String operationResult;
  private OperationalStatus status;
  private List<String> messages;

  public MaintenanceManagementInstanceInfo(OperationalStatus status) {
    this.status = status;
    this.messages = new ArrayList<>();
    this.operationResult = "";
  }

  public MaintenanceManagementInstanceInfo(OperationalStatus status, List<String> messages) {
    this.status = status;
    this.messages = messages;
    this.operationResult = "";
  }

  public MaintenanceManagementInstanceInfo(OperationalStatus status, String newOperationResult) {
    this.status = status;
    this.operationResult = newOperationResult;
    this.messages = new ArrayList<>();
  }

  public List<String> getMessages() {
    return messages;
  }

  public String getOperationResult() {
    return operationResult;
  }

  public boolean hasOperationResult() {
    return !operationResult.isEmpty();
  }

  public void setOperationResult(String result) {
    operationResult = result;
  }

  public void addMessages(List<String> msg) {
    messages.addAll(msg);
  }
  public void addMessage(String meg) {
    messages.add(meg);
  }

  public boolean isSuccessful() {
    return status.equals(OperationalStatus.SUCCESS);
  }

  public void mergeResult(MaintenanceManagementInstanceInfo info) {
    mergeResult(info, false);
  }

  public void mergeResult(MaintenanceManagementInstanceInfo info, boolean nonBlockingFailure) {
    messages.addAll(info.getMessages());
    status =
        (info.isSuccessful() || nonBlockingFailure) && isSuccessful() ? OperationalStatus.SUCCESS
            : OperationalStatus.FAILURE;
    if (info.hasOperationResult()) {
      operationResult =
          this.hasOperationResult() ? operationResult + "," + info.getOperationResult()
              : info.getOperationResult();
    }
  }
}