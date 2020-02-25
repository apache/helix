package org.apache.helix.zookeeper.impl.client;

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestSharedZkClient extends RealmAwareZkClientTestBase {
  @BeforeClass
  public void beforeClass()
      throws Exception {
    super.beforeClass();
    // Set the factory to SharedZkClientFactory
    _realmAwareZkClientFactory = SharedZkClientFactory.getInstance();
  }

  @Test
  public void testCreateEphemeralFailure() {
    _realmAwareZkClient.setZkSerializer(new ZNRecordSerializer());

    // Create a dummy ZNRecord
    ZNRecord znRecord = new ZNRecord("DummyRecord");
    znRecord.setSimpleField("Dummy", "Value");

    // test createEphemeral should fail
    try {
      _realmAwareZkClient.createEphemeral(TEST_VALID_PATH);
      Assert.fail("sharedReamlAwareZkClient is not expected to be able to create ephemeral node via createEphemeral");
    } catch (UnsupportedOperationException e){
      // this is expected
    }

    // test creating Ephemeral via creat would also fail
    try {
      _realmAwareZkClient.create(TEST_VALID_PATH, znRecord, CreateMode.EPHEMERAL);
      Assert.fail("sharedRealmAwareZkClient is not expected to be able to create ephmeral node via create");
    } catch (UnsupportedOperationException e) {
      // this is expected.
    }
  }

}