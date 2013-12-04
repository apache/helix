package org.apache.helix.controller.rebalancer.context;

import java.util.Map;

import org.apache.helix.api.Partition;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.config.CustomRebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.helix.model.ResourceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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
 * Ensure that a RebalancerContext of a specified type is able to be serialized and deserialized.
 */
public class TestSerializeRebalancerContext {
  @Test
  public void basicTest() {
    // populate a context
    CustomRebalancerConfig context = new CustomRebalancerConfig();
    context.setAnyLiveParticipant(false);
    context.setMaxPartitionsPerParticipant(Integer.MAX_VALUE);
    Map<PartitionId, Partition> partitionMap = Maps.newHashMap();
    ResourceId resourceId = ResourceId.from("testResource");
    PartitionId partitionId = PartitionId.from(resourceId, "0");
    partitionMap.put(partitionId, new Partition(partitionId));
    context.setPartitionMap(partitionMap);
    Map<PartitionId, Map<ParticipantId, State>> preferenceMaps = Maps.newHashMap();
    ParticipantId participant1 = ParticipantId.from("participant1");
    ParticipantId participant2 = ParticipantId.from("participant2");
    Map<ParticipantId, State> preferenceMap =
        ImmutableMap.of(participant1, State.from("MASTER"), participant2, State.from("SLAVE"));
    preferenceMaps.put(partitionId, preferenceMap);
    context.setPreferenceMaps(preferenceMaps);
    context.setReplicaCount(3);
    context.setStateModelDefId(StateModelDefId.from("MasterSlave"));
    context.setResourceId(resourceId);

    // serialize and deserialize by wrapping in a config
    RebalancerConfigHolder config = new RebalancerConfigHolder(context);
    CustomRebalancerConfig deserialized = config.getRebalancerConfig(CustomRebalancerConfig.class);

    // check to make sure that the two objects contain the same data
    Assert.assertNotNull(deserialized);
    Assert.assertEquals(deserialized.anyLiveParticipant(), context.anyLiveParticipant());
    Assert.assertEquals(deserialized.getPreferenceMap(partitionId).get(participant1), context
        .getPreferenceMap(partitionId).get(participant1));
    Assert.assertEquals(deserialized.getPreferenceMap(partitionId).get(participant2), context
        .getPreferenceMap(partitionId).get(participant2));
    Assert.assertEquals(deserialized.getReplicaCount(), context.getReplicaCount());
    Assert.assertEquals(deserialized.getStateModelDefId(), context.getStateModelDefId());
    Assert.assertEquals(deserialized.getResourceId(), context.getResourceId());

    // wrap in a physical config and then unwrap it
    ResourceConfiguration physicalConfig = new ResourceConfiguration(resourceId);
    physicalConfig.addNamespacedConfig(config.toNamespacedConfig());
    RebalancerConfigHolder extractedConfig = new RebalancerConfigHolder(physicalConfig);
    CustomRebalancerConfig extractedContext =
        extractedConfig.getRebalancerConfig(CustomRebalancerConfig.class);

    // make sure the unwrapped data hasn't changed
    Assert.assertNotNull(extractedContext);
    Assert.assertEquals(extractedContext.anyLiveParticipant(), context.anyLiveParticipant());
    Assert.assertEquals(extractedContext.getPreferenceMap(partitionId).get(participant1), context
        .getPreferenceMap(partitionId).get(participant1));
    Assert.assertEquals(extractedContext.getPreferenceMap(partitionId).get(participant2), context
        .getPreferenceMap(partitionId).get(participant2));
    Assert.assertEquals(extractedContext.getReplicaCount(), context.getReplicaCount());
    Assert.assertEquals(extractedContext.getStateModelDefId(), context.getStateModelDefId());
    Assert.assertEquals(extractedContext.getResourceId(), context.getResourceId());

    // make sure that it's legal to use a base rebalancer context
    RebalancerConfig rebalancerContext =
        extractedConfig.getRebalancerConfig(RebalancerConfig.class);
    Assert.assertNotNull(rebalancerContext);
    Assert.assertEquals(rebalancerContext.getResourceId(), context.getResourceId());
  }
}
