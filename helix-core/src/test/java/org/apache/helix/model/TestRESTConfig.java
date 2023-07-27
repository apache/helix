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
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixException;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestRESTConfig {
  @Test
  public void testGetBaseUrlValid() {
    ZNRecord record = new ZNRecord("test");
    record.setSimpleField(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL.name(), "http://*:8080");
    RESTConfig restConfig = new RESTConfig(record);
    Assert.assertEquals(restConfig.getBaseUrl("instance0"), "http://instance0:8080");
    Assert.assertEquals(restConfig.getBaseUrl("instance1_9090"), "http://instance1:8080");
  }

  @Test(expectedExceptions = HelixException.class)
  public void testGetBaseUrlInvalid() {
    ZNRecord record = new ZNRecord("test");
    record.setSimpleField(RESTConfig.SimpleFields.CUSTOMIZED_HEALTH_URL.name(), "http://foo:8080");
    RESTConfig restConfig = new RESTConfig(record);
    restConfig.getBaseUrl("instance0");
  }
}
