package org.apache.helix.filestore;

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

import org.apache.helix.HelixManager;
import org.apache.helix.participant.statemachine.StateModelFactory;

public class FileStoreStateModelFactory extends StateModelFactory<FileStoreStateModel> {
  private final HelixManager manager;

  public FileStoreStateModelFactory(HelixManager manager) {
    this.manager = manager;
  }

  @Override
  public FileStoreStateModel createNewStateModel(String resource, String partition) {
    FileStoreStateModel model;
    model = new FileStoreStateModel(manager, partition.split("_")[0], partition);
    return model;
  }
}
