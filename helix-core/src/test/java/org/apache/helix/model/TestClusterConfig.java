package org.apache.helix.model;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestClusterConfig {

  @Test
  public void testGetZoneId() {
    ClusterConfig clusterConfig = new ClusterConfig("test");
    clusterConfig.setTopology("/zone/rack/host/instance");

    String[] levels = clusterConfig.getTopologyLevel();

    Assert.assertEquals(levels.length, 4);
  }
}
