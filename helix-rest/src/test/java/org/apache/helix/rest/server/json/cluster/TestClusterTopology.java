package org.apache.helix.rest.server.json.cluster;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestClusterTopology {

  @Test
  public void whenSerializingClusterTopology() throws IOException {
    List<ClusterTopology.Instance> instances =
        ImmutableList.of(new ClusterTopology.Instance("instance"));

    List<ClusterTopology.Zone> zones = ImmutableList.of(new ClusterTopology.Zone("zone", instances));

    ClusterTopology clusterTopology =
        new ClusterTopology("cluster0", zones, Collections.emptySet());
    ObjectMapper mapper = new ObjectMapper();
    String result = mapper.writeValueAsString(clusterTopology);

    Assert.assertEquals(result,
        "{\"id\":\"cluster0\",\"zones\":[{\"id\":\"zone\",\"instances\":[{\"id\":\"instance\"}]}],\"allInstances\":[]}");
  }
}
