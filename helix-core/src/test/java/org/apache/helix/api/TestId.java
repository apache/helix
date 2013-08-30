package org.apache.helix.api;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestId {
  /**
   * Make sure that a partition serializes and deserializes properly
   */
  @Test
  public void testPartitionId() {
    final String partitionName = "Resource_3";
    final String resourceName = "Resource";
    final String partitionSuffix = "3";
    PartitionId partitionId = Id.partition(partitionName);
    Assert.assertEquals(partitionId.stringify(), partitionName);
    PartitionId partitionId2 = Id.partition(Id.resource(resourceName), partitionSuffix);
    Assert.assertEquals(partitionId2.stringify(), partitionName);
    Assert.assertEquals(partitionId, partitionId2);
    Assert.assertEquals(partitionId.toString(), partitionId2.toString());
  }

  /**
   * Check that PartitionId doesn't need to be of the form resource_partition for compatibility
   */
  @Test
  public void testPartitionIdCompatibility() {
    final String partitionName = "Resource--3";
    PartitionId partitionId = Id.partition(partitionName);
    Assert.assertEquals(partitionId.stringify(), partitionName);
  }

  /**
   * Check that ids can be converted back and forth between strings and concrete classes
   */
  @Test
  public void basicIdTest() {
    final String resourceName = "Resource";
    final String clusterName = "Cluster";
    final String participantName = "Participant";
    final String sessionName = "Session";
    final String processName = "Process";
    final String stateModelName = "StateModel";
    final String messageName = "Message";
    Assert.assertEquals(Id.resource(resourceName).stringify(), resourceName);
    Assert.assertEquals(Id.cluster(clusterName).stringify(), clusterName);
    Assert.assertEquals(Id.participant(participantName).stringify(), participantName);
    Assert.assertEquals(Id.session(sessionName).stringify(), sessionName);
    Assert.assertEquals(Id.process(processName).stringify(), processName);
    Assert.assertEquals(Id.stateModelDef(stateModelName).stringify(), stateModelName);
    Assert.assertEquals(Id.message(messageName).stringify(), messageName);
  }

  /**
   * Check that equality with string works
   */
  @Test
  public void testStringEquality() {
    final String resourceName = "Resource";
    Assert.assertTrue(Id.resource(resourceName).equals(resourceName));
  }

  /**
   * Ensure that trying to create an id with null yields null
   */
  @Test
  public void testNull() {
    Assert.assertNull(Id.cluster(null));
    Assert.assertNull(Id.resource(null));
    Assert.assertNull(Id.partition(null));
    Assert.assertNull(Id.participant(null));
    Assert.assertNull(Id.session(null));
    Assert.assertNull(Id.process(null));
    Assert.assertNull(Id.stateModelDef(null));
    Assert.assertNull(Id.message(null));
  }
}
