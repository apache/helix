package org.apache.helix.api.accessor;

import java.util.concurrent.TimeUnit;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
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

public class TestAccessorRecreate extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestAccessorRecreate.class);

  /**
   * This test just makes sure that a cluster is only recreated if it is incomplete. This is not
   * directly testing atomicity, but rather a use case where a machine died while creating the
   * cluster.
   */
  @Test
  public void testRecreateCluster() {
    final String MODIFIER = "modifier";
    final ClusterId clusterId = ClusterId.from("TestAccessorRecreate!testCluster");

    // connect
    boolean connected = _zkclient.waitUntilConnected(30000, TimeUnit.MILLISECONDS);
    if (!connected) {
      LOG.warn("Connection not established");
      return;
    }
    HelixDataAccessor helixAccessor = new ZKHelixDataAccessor(clusterId.stringify(), _baseAccessor);
    ClusterAccessor accessor = new ClusterAccessor(clusterId, helixAccessor);

    // create a cluster
    boolean created = createCluster(clusterId, accessor, MODIFIER, 1);
    Assert.assertTrue(created);

    // read the cluster
    Cluster clusterSnapshot = accessor.readCluster();
    Assert.assertEquals(clusterSnapshot.getUserConfig().getIntField(MODIFIER, -1), 1);

    // create a cluster with the same id
    boolean created2 = createCluster(clusterId, accessor, MODIFIER, 2);
    Assert.assertFalse(created2); // should fail since cluster exists

    // remove a required property
    helixAccessor.removeProperty(helixAccessor.keyBuilder().liveInstances());

    // try again, should work this time
    created2 = createCluster(clusterId, accessor, MODIFIER, 2);
    Assert.assertTrue(created2);

    // read the cluster again
    clusterSnapshot = accessor.readCluster();
    Assert.assertEquals(clusterSnapshot.getUserConfig().getIntField(MODIFIER, -1), 2);

    accessor.dropCluster();
  }

  /**
   * This test just makes sure that a participant is only recreated if it is incomplete. This is not
   * directly testing atomicity, but rather a use case where a machine died while creating the
   * participant.
   */
  @Test
  public void testRecreateParticipant() {
    final String MODIFIER = "modifier";
    final ClusterId clusterId = ClusterId.from("testCluster");
    final ParticipantId participantId = ParticipantId.from("testParticipant");

    // connect
    boolean connected = _zkclient.waitUntilConnected(30000, TimeUnit.MILLISECONDS);
    if (!connected) {
      LOG.warn("Connection not established");
      return;
    }
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
    HelixDataAccessor helixAccessor = new ZKHelixDataAccessor(clusterId.stringify(), baseAccessor);
    ClusterAccessor accessor = new ClusterAccessor(clusterId, helixAccessor);

    // create the cluster
    boolean clusterCreated = createCluster(clusterId, accessor, MODIFIER, 0);
    Assert.assertTrue(clusterCreated);

    // create the participant
    boolean created = createParticipant(participantId, accessor, MODIFIER, 1);
    Assert.assertTrue(created);

    // read the participant
    Participant participantSnapshot = accessor.readParticipant(participantId);
    Assert.assertEquals(participantSnapshot.getUserConfig().getIntField(MODIFIER, -1), 1);

    // create a participant with the same id
    boolean created2 = createParticipant(participantId, accessor, MODIFIER, 2);
    Assert.assertFalse(created2); // should fail since participant exists

    // remove a required property
    helixAccessor.removeProperty(helixAccessor.keyBuilder().messages(participantId.stringify()));

    // try again, should work this time
    created2 = createParticipant(participantId, accessor, MODIFIER, 2);
    Assert.assertTrue(created2);

    // read the cluster again
    participantSnapshot = accessor.readParticipant(participantId);
    Assert.assertEquals(participantSnapshot.getUserConfig().getIntField(MODIFIER, -1), 2);

    accessor.dropCluster();
  }

  private boolean createCluster(ClusterId clusterId, ClusterAccessor accessor, String modifierName,
      int modifierValue) {
    // create a cluster
    UserConfig userConfig = new UserConfig(Scope.cluster(clusterId));
    userConfig.setIntField(modifierName, modifierValue);
    ClusterConfig cluster = new ClusterConfig.Builder(clusterId).userConfig(userConfig).build();
    return accessor.createCluster(cluster);
  }

  private boolean createParticipant(ParticipantId participantId, ClusterAccessor accessor,
      String modifierName, int modifierValue) {
    // create a participant
    UserConfig userConfig = new UserConfig(Scope.participant(participantId));
    userConfig.setIntField(modifierName, modifierValue);
    ParticipantConfig participant =
        new ParticipantConfig.Builder(participantId).hostName("host").port(0)
            .userConfig(userConfig).build();
    return accessor.addParticipant(participant);
  }
  // private HelixLockable lockProvider() {
  // return new HelixLockable() {
  // @Override
  // public HelixLock getLock(ClusterId clusterId, Scope<?> scope) {
  // return new ZKHelixLock(clusterId, scope, _gZkClient);
  // }
  // };
  // }
}
