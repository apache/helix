package org.apache.helix.rest.server.json.cluster;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterInfo {
  @Test
  public void whenSerializingClusterInfo() throws JsonProcessingException {
    ClusterInfo clusterInfo = new ClusterInfo.Builder("cluster0")
        .controller("controller")
        .idealStates(ImmutableList.of("idealState0"))
        .instances(ImmutableList.of("instance0"))
        .maintenance(true)
        .paused(true)
        .liveInstances(ImmutableList.of("instance0"))
        .build();
    ObjectMapper mapper = new ObjectMapper();
    String result = mapper.writeValueAsString(clusterInfo);

    Assert.assertEquals(mapper.readTree(result),
                        mapper.readTree("{\"id\":\"cluster0\",\"controller\":\"controller\",\"paused\":true,\"maintenance\":true,\"resources\":[\"idealState0\"],\"instances\":[\"instance0\"],\"liveInstances\":[\"instance0\"]}"));
  }
}
