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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStoppableCheck {

  @Test
  public void whenSerializingStoppableCheck() throws JsonProcessingException {
    StoppableCheck stoppableCheck = new StoppableCheck(false, ImmutableList.of("check"),
        StoppableCheck.Category.HELIX_OWN_CHECK);

    ObjectMapper mapper = new ObjectMapper();
    String result = mapper.writeValueAsString(stoppableCheck);

    Assert.assertEquals(result, "{\"stoppable\":false,\"failedChecks\":[\"Helix:check\"]}");
  }

  @Test
  public void testConstructorSortingOrder() {
    StoppableCheck stoppableCheck =
        new StoppableCheck(ImmutableMap.of("a", true, "c", false, "b", false),
            StoppableCheck.Category.HELIX_OWN_CHECK);
    Assert.assertFalse(stoppableCheck.isStoppable());
    Assert.assertEquals(stoppableCheck.getFailedChecks(), ImmutableList.of("Helix:b", "Helix:c"));
  }
}
