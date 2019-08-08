package org.apache.helix.webapp.resources;

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

import java.util.Map;

import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestJsonParameters {
  @Test
  public void test() throws Exception {
    String jsonPayload =
        "{\"command\":\"resetPartition\",\"resource\": \"DB-1\",\"partition\":\"DB-1_22 DB-1_23\"}";
    Map<String, String> map = ClusterRepresentationUtil.JsonToMap(jsonPayload);
    Assert.assertNotNull(map.get(JsonParameters.MANAGEMENT_COMMAND));
    Assert.assertEquals(ClusterSetup.resetPartition, map.get(JsonParameters.MANAGEMENT_COMMAND));
    Assert.assertNotNull(map.get(JsonParameters.RESOURCE));
    Assert.assertEquals("DB-1", map.get(JsonParameters.RESOURCE));
    Assert.assertNotNull(map.get(JsonParameters.PARTITION));
    Assert.assertEquals("DB-1_22 DB-1_23", map.get(JsonParameters.PARTITION));
  }

}
