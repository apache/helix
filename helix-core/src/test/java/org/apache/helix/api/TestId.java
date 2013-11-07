package org.apache.helix.api;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ProcId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
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
    PartitionId partitionId = PartitionId.from(partitionName);
    Assert.assertEquals(partitionId.stringify(), partitionName);
    PartitionId partitionId2 = PartitionId.from(ResourceId.from(resourceName), partitionSuffix);
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
    PartitionId partitionId = PartitionId.from(partitionName);
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
    final String stateModelFactoryName = "StateModelFactory";
    final String messageName = "Message";
    Assert.assertEquals(ResourceId.from(resourceName).stringify(), resourceName);
    Assert.assertEquals(ClusterId.from(clusterName).stringify(), clusterName);
    Assert.assertEquals(ParticipantId.from(participantName).stringify(), participantName);
    Assert.assertEquals(SessionId.from(sessionName).stringify(), sessionName);
    Assert.assertEquals(ProcId.from(processName).stringify(), processName);
    Assert.assertEquals(StateModelDefId.from(stateModelName).stringify(), stateModelName);
    Assert.assertEquals(StateModelFactoryId.from(stateModelFactoryName).stringify(),
        stateModelFactoryName);
    Assert.assertEquals(MessageId.from(messageName).stringify(), messageName);
  }

  /**
   * Check that equality with string works
   */
  @Test
  public void testStringEquality() {
    final String resourceName = "Resource";
    Assert.assertTrue(ResourceId.from(resourceName).equals(resourceName));
  }

  /**
   * Ensure that trying to create an id with null yields null
   */
  @Test
  public void testNull() {
    Assert.assertNull(ClusterId.from(null));
    Assert.assertNull(ResourceId.from(null));
    Assert.assertNull(PartitionId.from(null));
    Assert.assertNull(ParticipantId.from(null));
    Assert.assertNull(SessionId.from(null));
    Assert.assertNull(ProcId.from(null));
    Assert.assertNull(StateModelDefId.from(null));
    Assert.assertNull(StateModelFactoryId.from(null));
    Assert.assertNull(MessageId.from(null));
  }
}
