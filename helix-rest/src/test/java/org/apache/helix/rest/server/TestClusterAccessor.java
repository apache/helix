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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.helix.rest.server.resources.ClusterAccessor;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterAccessor extends AbstractTestClass {
  ObjectMapper _mapper = new ObjectMapper();

  @Test
  public void testGetClusters() throws IOException {
    final Response response = target("clusters").request().get();
    Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertEquals(response.getMediaType().getType(), "text");

    String body = response.readEntity(String.class);
    Assert.assertNotNull(body);

    JsonNode node = _mapper.readTree(body);
    String clustersStr = node.get(ClusterAccessor.ClusterProperties.clusters.name()).toString();
    Assert.assertNotNull(clustersStr);

    Set<String> clusters = _mapper.readValue(clustersStr,
        _mapper.getTypeFactory().constructCollectionType(Set.class, String.class));
    Assert.assertEquals(clusters, _clusters,
        "clusters from response: " + clusters + " vs clusters actually: " + _clusters);
  }
}
