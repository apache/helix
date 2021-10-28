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

  public enum Status {
    SUCCESS,
    FAILURE
  }
  private List<Object> operationResult;
  private Status status;
  private List<String> messages;

  public MaintenanceManagementInstanceInfo(Status status) {
    this.status = status;
    this.operationResult = new ArrayList<>();
    this.messages = new ArrayList<>();
  }

  public MaintenanceManagementInstanceInfo(Status status, List<Object> newoperationResult) {
    this.status = status;
    this.operationResult = new ArrayList<>(newoperationResult);
    this.messages = new ArrayList<>();
  }

  public List<String> getMessages(){
    return messages;
  }

  public List<Object> getOperationResult(){
    return operationResult;
  }

  public void addFailureMessage(List<String> msg) {
    messages.addAll(msg);
  }

  public boolean isSuccessful() {return status.equals(Status.SUCCESS);}

  public void mergeResult(MaintenanceManagementInstanceInfo info) {
    operationResult.addAll(info.getOperationResult());
    messages.addAll(info.getMessages());
    status =  info.isSuccessful() && isSuccessful() ? Status.SUCCESS : Status.FAILURE;
  }

}