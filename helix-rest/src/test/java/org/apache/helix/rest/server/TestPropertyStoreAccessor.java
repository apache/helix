package org.apache.helix.rest.server;

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

import org.apache.helix.rest.server.util.JerseyUriRequestBuilder;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TestPropertyStoreAccessor extends AbstractTestClass {
  private static final String TEST_CLUSTER = "TestCluster_0";

  @Test
  public void testGetPropertyStore() {
    String path = "/TaskRebalancer/Workflow_0/Context";
    String result = new JerseyUriRequestBuilder("clusters/{}/propertyStore" + path)
        .format(TEST_CLUSTER).isBodyReturnExpected(true).get(this);
    Assert.assertFalse(result.isEmpty());
  }

  //TODO: implement the method
  @Test
  public void testGetPropertyStoreWithInValidPath() {
    String path = "/TaskRebalancer///Workflow_0/Context/";
    String result = new JerseyUriRequestBuilder("clusters/{}/propertyStore" + path)
        .format(TEST_CLUSTER).isBodyReturnExpected(true).expectedReturnStatusCode(400)
        .get(this);
    Assert.assertFalse(result.isEmpty());
  }
}
