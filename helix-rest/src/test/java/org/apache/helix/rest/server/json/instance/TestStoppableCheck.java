package org.apache.helix.rest.server.json.instance;

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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class TestStoppableCheck {

  @Test
  public void whenSerializingStoppableCheck() throws JsonProcessingException {
    StoppableCheck stoppableCheck = new StoppableCheck(false, ImmutableList.of("failedCheck"));

    ObjectMapper mapper = new ObjectMapper();
    String result = mapper.writeValueAsString(stoppableCheck);

    Assert.assertEquals(result, "{\"stoppable\":false,\"failedChecks\":[\"failedCheck\"]}");
  }

  @Test
  public void testMergeStoppableChecks() throws JsonProcessingException {
    Map<String, Boolean> helixCheck = ImmutableMap.of("check0", false, "check1", false);
    Map<String, Boolean> customCheck = ImmutableMap.of("check1", true, "check2", true);

    StoppableCheck stoppableCheck = StoppableCheck.mergeStoppableChecks(helixCheck, customCheck);
    ObjectMapper mapper = new ObjectMapper();
    String result = mapper.writeValueAsString(stoppableCheck);

    Assert.assertEquals(result, "{\"stoppable\":false,\"failedChecks\":[\"Helix:check1\",\"Helix:check0\"]}");
  }
}
