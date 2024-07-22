package org.apache.helix.gateway;

import org.apache.helix.gateway.base.HelixGatewayTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DummyTest extends HelixGatewayTestBase {

  @BeforeClass
  public void beforeClass() {
    _numParticipants = 5;
    super.beforeClass();
  }

  @Test
  public void testSetups() {
    createResource("TestDB_1", 4, 2);
    Assert.assertTrue(_clusterVerifier.verify());
  }
}
