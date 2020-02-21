package org.apache.helix.zookeeper.impl.client;

import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.testng.annotations.BeforeClass;


public class TestSharedZkClient extends RealmAwareZkClientTestBase {
  @BeforeClass
  public void beforeClass()
      throws Exception {
    super.beforeClass();
    // Set the factory to DedicatedZkClientFactory
    _realmAwareZkClientFactory = SharedZkClientFactory.getInstance();
  }
}
