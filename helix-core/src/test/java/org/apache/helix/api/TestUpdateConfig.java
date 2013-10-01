package org.apache.helix.api;

import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.FullAutoRebalancerContext;
import org.apache.helix.controller.rebalancer.context.SemiAutoRebalancerContext;
import org.testng.Assert;
import org.testng.annotations.Test;

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

/**
 * Testing the deltas of the various config classes. They should be able to update corresponding
 * configs correctly
 */
public class TestUpdateConfig {
  @Test
  public void testParticipantConfigUpdate() {
    final String ORIG_HOSTNAME = "host1";
    final String NEW_HOSTNAME = "host2";
    final int PORT = 1234;
    final String TAG1 = "tag1";
    final String TAG2 = "tag2";
    final String TAG3 = "tag3";
    final PartitionId partition1 = PartitionId.from("resource_1");
    final PartitionId partition2 = PartitionId.from("resource_2");
    final PartitionId partition3 = PartitionId.from("resource_3");
    final ParticipantId participantId = ParticipantId.from("participant");

    // start: add a user config, set host & port, add 2 tags and 2 disabled partition, start
    // disabled
    UserConfig userConfig = new UserConfig(Scope.participant(participantId));
    userConfig.setSimpleField("key1", "value1");
    ParticipantConfig config =
        new ParticipantConfig.Builder(participantId).hostName(ORIG_HOSTNAME).port(PORT)
            .enabled(false).addTag(TAG1).addTag(TAG2).addDisabledPartition(partition1)
            .addDisabledPartition(partition2).userConfig(userConfig).build();
    UserConfig newUserConfig = new UserConfig(Scope.participant(participantId));
    newUserConfig.setSimpleField("key2", "value2");

    // update: change host, remove a tag, add a tag, remove a disabled partition, add a disabled
    // partition, change user config
    ParticipantConfig updated =
        new ParticipantConfig.Delta(participantId).setHostName(NEW_HOSTNAME).removeTag(TAG1)
            .addTag(TAG3).removeDisabledPartition(partition1).addDisabledPartition(partition3)
            .setUserConfig(newUserConfig).mergeInto(config);
    Assert.assertEquals(updated.getHostName(), NEW_HOSTNAME);
    Assert.assertEquals(updated.getPort(), PORT);
    Assert.assertFalse(updated.hasTag(TAG1));
    Assert.assertTrue(updated.hasTag(TAG2));
    Assert.assertTrue(updated.hasTag(TAG3));
    Assert.assertFalse(updated.getDisabledPartitions().contains(partition1));
    Assert.assertTrue(updated.getDisabledPartitions().contains(partition2));
    Assert.assertTrue(updated.getDisabledPartitions().contains(partition3));
    Assert.assertNull(updated.getUserConfig().getSimpleField("key1"));
    Assert.assertEquals(updated.getUserConfig().getSimpleField("key2"), "value2");
    Assert.assertFalse(updated.isEnabled());
  }

  @Test
  public void testResourceConfigUpdate() {
    final int OLD_BUCKET_SIZE = 0;
    final int NEW_BUCKET_SIZE = 1;
    final ResourceId resourceId = ResourceId.from("resource");

    // start: add a user config, a semi auto rebalancer context, and set bucket size and batch
    // message mode
    UserConfig userConfig = new UserConfig(Scope.resource(resourceId));
    userConfig.setSimpleField("key1", "value1");
    SemiAutoRebalancerContext rebalancerContext =
        new SemiAutoRebalancerContext.Builder(resourceId).build();
    ResourceConfig config =
        new ResourceConfig.Builder(resourceId).userConfig(userConfig)
            .rebalancerContext(rebalancerContext).bucketSize(OLD_BUCKET_SIZE)
            .batchMessageMode(true).build();

    // update: overwrite user config, change to full auto rebalancer context, and change the bucket
    // size
    UserConfig newUserConfig = new UserConfig(Scope.resource(resourceId));
    newUserConfig.setSimpleField("key2", "value2");
    FullAutoRebalancerContext newRebalancerContext =
        new FullAutoRebalancerContext.Builder(resourceId).build();
    ResourceConfig updated =
        new ResourceConfig.Delta(resourceId).setBucketSize(NEW_BUCKET_SIZE)
            .setUserConfig(newUserConfig).setRebalancerContext(newRebalancerContext)
            .mergeInto(config);
    Assert.assertEquals(updated.getBucketSize(), NEW_BUCKET_SIZE);
    Assert.assertTrue(updated.getBatchMessageMode());
    Assert.assertNull(updated.getRebalancerConfig().getRebalancerContext(
        SemiAutoRebalancerContext.class));
    Assert.assertNotNull(updated.getRebalancerConfig().getRebalancerContext(
        FullAutoRebalancerContext.class));
    Assert.assertNull(updated.getUserConfig().getSimpleField("key1"));
    Assert.assertEquals(updated.getUserConfig().getSimpleField("key2"), "value2");
  }

  @Test
  public void testClusterConfigUpdate() {
    final ClusterId clusterId = ClusterId.from("cluster");
    final StateModelDefId masterSlave = StateModelDefId.from("MasterSlave");
    final State master = State.from("MASTER");
    final State slave = State.from("SLAVE");
    final State offline = State.from("OFFLINE");

    // start: add a user config, add master and slave constraints
    UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
    userConfig.setSimpleField("key1", "value1");
    ClusterConfig config =
        new ClusterConfig.Builder(clusterId)
            .addStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, master, 2)
            .addStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, slave, 3)
            .userConfig(userConfig).autoJoin(true).build();

    // update: overwrite user config, change master constraint, remove slave constraint, add offline
    // constraint, change auto join
    UserConfig newUserConfig = new UserConfig(Scope.cluster(clusterId));
    newUserConfig.setSimpleField("key2", "value2");
    ClusterConfig updated =
        new ClusterConfig.Delta(clusterId)
            .addStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, master, 1)
            .removeStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, slave)
            .addStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, offline, "R")
            .setUserConfig(newUserConfig).setAutoJoin(false).mergeInto(config);
    Assert.assertEquals(
        updated.getStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, master), "1");
    Assert.assertEquals(
        updated.getStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, slave), "-1");
    Assert.assertEquals(
        updated.getStateUpperBoundConstraint(Scope.cluster(clusterId), masterSlave, offline), "R");
    Assert.assertNull(updated.getUserConfig().getSimpleField("key1"));
    Assert.assertEquals(updated.getUserConfig().getSimpleField("key2"), "value2");
    Assert.assertFalse(updated.autoJoinAllowed());
  }
}
