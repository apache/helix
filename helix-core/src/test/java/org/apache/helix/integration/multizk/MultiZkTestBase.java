package org.apache.helix.integration.multizk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableList;
import org.apache.helix.HelixAdmin;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

/**
 * This class sets up the clusters and zk servers for multiple zk server testing.
 */
public class MultiZkTestBase {
    protected static final int NUM_ZK = 3;
    protected static final Map<String, ZkServer> ZK_SERVER_MAP = new HashMap<>();
    protected static final Map<String, HelixZkClient> ZK_CLIENT_MAP = new HashMap<>();
    protected static final Map<String, ClusterControllerManager> MOCK_CONTROLLERS = new HashMap<>();
    protected final Set<MockParticipantManager> MOCK_PARTICIPANTS = new HashSet<>();
    protected static final List<String> CLUSTER_LIST =
            ImmutableList.of("CLUSTER_1", "CLUSTER_2", "CLUSTER_3");

    protected MockMetadataStoreDirectoryServer _msds;
    protected static final Map<String, Collection<String>> _rawRoutingData = new HashMap<>();
    protected RealmAwareZkClient _zkClient;
    protected HelixAdmin _zkHelixAdmin;

    // Save System property configs from before this test and pass onto after the test
    protected final Map<String, String> _configStore = new HashMap<>();

    protected static final String ZK_PREFIX = "localhost:";
    protected static final int ZK_START_PORT = 8977;
    protected String _msdsEndpoint;

    @BeforeClass
    public void beforeClass() throws Exception {
        // Create 3 in-memory zookeepers and routing mapping
        for (int i = 0; i < NUM_ZK; i++) {
            String zkAddress = ZK_PREFIX + (ZK_START_PORT + i);
            ZK_SERVER_MAP.put(zkAddress, TestHelper.startZkServer(zkAddress));
            ZK_CLIENT_MAP.put(zkAddress, TestHelper.createZkClient(zkAddress));

            // One cluster per ZkServer created
            _rawRoutingData.put(zkAddress, Collections.singletonList("/" + CLUSTER_LIST.get(i)));
        }

        // Create a Mock MSDS
        final String msdsHostName = "localhost";
        final int msdsPort = 11117;
        final String msdsNamespace = "multiZkTest";
        _msdsEndpoint =
                "http://" + msdsHostName + ":" + msdsPort + "/admin/v2/namespaces/" + msdsNamespace;
        _msds = new MockMetadataStoreDirectoryServer(msdsHostName, msdsPort, msdsNamespace,
                _rawRoutingData);
        _msds.startServer();

        // Save previously-set system configs
        String prevMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
        String prevMsdsServerEndpoint =
                System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
        if (prevMultiZkEnabled != null) {
            _configStore.put(SystemPropertyKeys.MULTI_ZK_ENABLED, prevMultiZkEnabled);
        }
        if (prevMsdsServerEndpoint != null) {
            _configStore
                    .put(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, prevMsdsServerEndpoint);
        }

        // Turn on multiZk mode in System config
        System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "true");
    }

    @AfterClass
    public void afterClass() throws Exception {

        try {
            // Kill all mock controllers and participants
            MOCK_CONTROLLERS.values().forEach(ClusterControllerManager::syncStop);
            if (!MOCK_PARTICIPANTS.isEmpty()){
                MOCK_PARTICIPANTS.forEach(mockParticipantManager -> {
                    mockParticipantManager.syncStop();
                    StateMachineEngine stateMachine = mockParticipantManager.getStateMachineEngine();
                    if (stateMachine != null) {
                        StateModelFactory stateModelFactory = stateMachine.getStateModelFactory("Task");
                        if (stateModelFactory instanceof TaskStateModelFactory) {
                            ((TaskStateModelFactory) stateModelFactory).shutdown();
                        }
                    }
                });
            }

            // Tear down all clusters
            CLUSTER_LIST.forEach(cluster -> TestHelper.dropCluster(cluster, _zkClient));

            // Verify that all clusters are gone in each zookeeper
            Assert.assertTrue(TestHelper.verify(() -> {
                for (Map.Entry<String, HelixZkClient> zkClientEntry : ZK_CLIENT_MAP.entrySet()) {
                    List<String> children = zkClientEntry.getValue().getChildren("/");
                    if (children.stream().anyMatch(CLUSTER_LIST::contains)) {
                        return false;
                    }
                }
                return true;
            }, TestHelper.WAIT_DURATION));

        } finally {
            // Tear down zookeepers
            ZK_CLIENT_MAP.forEach((zkAddress, zkClient) -> zkClient.close());
            ZK_SERVER_MAP.forEach((zkAddress, zkServer) -> zkServer.shutdown());

            // Stop MockMSDS
            _msds.stopServer();

            // Close ZK client connections
            if (_zkHelixAdmin != null) {
                _zkHelixAdmin.close();
            }
            if (_zkClient != null && !_zkClient.isClosed()) {
                _zkClient.close();
            }

            // Restore System property configs
            if (_configStore.containsKey(SystemPropertyKeys.MULTI_ZK_ENABLED)) {
                System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED,
                        _configStore.get(SystemPropertyKeys.MULTI_ZK_ENABLED));
            } else {
                System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
            }
            if (_configStore.containsKey(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY)) {
                System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY,
                        _configStore.get(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY));
            } else {
                System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
            }
        }
    }
}
