package org.apache.helix.rest.server.service;

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

import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertyStoreService {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyStoreService.class);
  private HelixZkClient _zkClient;

  public PropertyStoreService(HelixZkClient zkClient) {
    _zkClient = zkClient;
  }

  public ZNRecord readRecord(String path) {
    // throw exception instead of returning null
    Object content = _zkClient.readData(path, false);
    try {
      return ZNRecord.class.cast(content);
    } catch (ClassCastException e) {
      LOG.warn("The content of node at path {} is not ZNRecord format", path);
    }

    // fallback to a default and simple ZNRecord
    ZNRecord znRecord = new ZNRecord(path);
    znRecord.setSimpleField(path, content.toString());
    return znRecord;
  }
}
