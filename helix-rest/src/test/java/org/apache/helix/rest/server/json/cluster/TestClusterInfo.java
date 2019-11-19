package org.apache.helix.rest.server.json.cluster;

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

    Assert.assertEquals(result,
        "{\"id\":\"cluster0\",\"controller\":\"controller\",\"paused\":true,\"maintenance\":true,\"resources\":[\"idealState0\"],\"instances\":[\"instance0\"],\"liveInstances\":[\"instance0\"]}");
  }
}
