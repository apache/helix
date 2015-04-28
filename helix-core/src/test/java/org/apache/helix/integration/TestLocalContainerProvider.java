package org.apache.helix.integration;

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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixController;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixParticipant;
import org.apache.helix.TestHelper;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.accessor.ClusterAccessor;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerProvider;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.ProvisionerRef;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.controller.serializer.DefaultStringSerializer;
import org.apache.helix.controller.serializer.StringSerializer;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.AutoRebalanceModeISBuilder;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;
import org.codehaus.jackson.annotate.JsonProperty;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

public class TestLocalContainerProvider extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestLocalContainerProvider.class);

  private static final int MAX_PARTICIPANTS = 4;
  static String clusterName = null;
  static String resourceName = null;
  static volatile int allocated = 0;
  static volatile int started = 0;
  static volatile int stopped = 0;
  static volatile int deallocated = 0;
  static HelixConnection connection = null;
  static CountDownLatch latch = new CountDownLatch(MAX_PARTICIPANTS);

  @Test
  public void testBasic() throws Exception {
    final int NUM_PARTITIONS = 4;
    final int NUM_REPLICAS = 2;
    resourceName = "TestDB0";

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    clusterName = className + "_" + methodName;
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    allocated = 0;
    started = 0;
    stopped = 0;
    deallocated = 0;

    // connect
    connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // create the cluster
    ClusterId clusterId = ClusterId.from(clusterName);
    ClusterAccessor clusterAccessor = connection.createClusterAccessor(clusterId);
    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    clusterAccessor.createCluster(new ClusterConfig.Builder(clusterId).addStateModelDefinition(
        masterSlave).build());

    // add the resource with the local provisioner
    ResourceId resourceId = ResourceId.from(resourceName);
    ProvisionerConfig provisionerConfig = new LocalProvisionerConfig(resourceId);
    AutoRebalanceModeISBuilder idealStateBuilder = new AutoRebalanceModeISBuilder(resourceId);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      idealStateBuilder.add(PartitionId.from(resourceId, String.valueOf(i)));
    }
    idealStateBuilder.setNumReplica(NUM_REPLICAS).setStateModelDefId(
        masterSlave.getStateModelDefId());
    clusterAccessor.addResource(new ResourceConfig.Builder(ResourceId.from(resourceName))
        .provisionerConfig(provisionerConfig).idealState(idealStateBuilder.build()).build());

    // start controller
    ControllerId controllerId = ControllerId.from("controller1");
    HelixController controller = connection.createController(clusterId, controllerId);
    controller.start();

    latch.await(30000, TimeUnit.MILLISECONDS);

    // clean up
    controller.stop();
    connection.disconnect();

    Assert.assertEquals(allocated, MAX_PARTICIPANTS);
    Assert.assertEquals(started, MAX_PARTICIPANTS);
    Assert.assertEquals(stopped, MAX_PARTICIPANTS);
    Assert.assertEquals(deallocated, MAX_PARTICIPANTS);
  }

  /**
   * Use Guava's service to wrap a participant lifecycle
   */
  public static class ParticipantService extends AbstractService {
    private final ClusterId _clusterId;
    private final ParticipantId _participantId;
    private HelixParticipant _participant;

    public ParticipantService(ClusterId clusterId, ParticipantId participantId) {
      // TODO: probably should pass a connection in here
      _clusterId = clusterId;
      _participantId = participantId;
    }

    @Override
    protected void doStart() {
      _participant = connection.createParticipant(_clusterId, _participantId);
      _participant.getStateMachineEngine().registerStateModelFactory(
          StateModelDefId.from("MasterSlave"), new TestHelixConnection.MockStateModelFactory());
      _participant.start();
      notifyStarted();
    }

    @Override
    protected void doStop() {
      _participant.stop();
      notifyStopped();
    }

  }

  /**
   * Bare-bones ProvisionerConfig
   */
  public static class LocalProvisionerConfig implements ProvisionerConfig {
    private ResourceId _resourceId;
    private Class<? extends StringSerializer> _serializerClass;
    private ProvisionerRef _provisionerRef;

    public LocalProvisionerConfig(@JsonProperty("resourceId") ResourceId resourceId) {
      _resourceId = resourceId;
      _serializerClass = DefaultStringSerializer.class;
      _provisionerRef = ProvisionerRef.from(LocalProvisioner.class.getName());
    }

    @Override
    public ResourceId getResourceId() {
      return _resourceId;
    }

    @Override
    public ProvisionerRef getProvisionerRef() {
      return _provisionerRef;
    }

    public void setProvisionerRef(ProvisionerRef provisionerRef) {
      _provisionerRef = provisionerRef;
    }

    @Override
    public Class<? extends StringSerializer> getSerializerClass() {
      return _serializerClass;
    }

    public void setSerializerClass(Class<? extends StringSerializer> serializerClass) {
      _serializerClass = serializerClass;
    }
  }

  /**
   * Provisioner that will start and stop participants locally
   */
  public static class LocalProvisioner implements Provisioner, TargetProvider, ContainerProvider {
    private HelixManager _helixManager;
    private ClusterId _clusterId;
    private int _askCount;
    private Map<ContainerId, ContainerState> _states;
    private Map<ContainerId, ParticipantId> _containerParticipants;
    private Map<ContainerId, ParticipantService> _participants;

    @Override
    public void init(HelixManager helixManager, ResourceConfig resourceConfig) {
      // TODO: would be nice to have a HelixConnection instead of a HelixManager
      _helixManager = helixManager;
      _clusterId = ClusterId.from(_helixManager.getClusterName());
      _askCount = 0;
      _states = Maps.newHashMap();
      _containerParticipants = Maps.newHashMap();
      _participants = Maps.newHashMap();
    }

    @Override
    public ListenableFuture<ContainerId> allocateContainer(ContainerSpec spec) {
      // allocation is a no-op
      ContainerId containerId = ContainerId.from(spec.getParticipantId().toString());
      _states.put(containerId, ContainerState.ACQUIRED);
      _containerParticipants.put(containerId, spec.getParticipantId());
      allocated++;
      LOG.info(String.format("ALLOC: %d %d %d %d", allocated, started, stopped, deallocated));
      SettableFuture<ContainerId> future = SettableFuture.create();
      future.set(containerId);
      return future;
    }

    @Override
    public ListenableFuture<Boolean> deallocateContainer(ContainerId containerId) {
      // deallocation is a no-op
      _states.put(containerId, ContainerState.FINALIZED);
      deallocated++;
      LOG.info(String.format("DEALLOC: %d %d %d %d", allocated, started, stopped, deallocated));
      latch.countDown();
      SettableFuture<Boolean> future = SettableFuture.create();
      future.set(true);
      return future;
    }

    @Override
    public ListenableFuture<Boolean> startContainer(ContainerId containerId, Participant participant) {
      ParticipantService participantService =
          new ParticipantService(_clusterId, _containerParticipants.get(containerId));
      participantService.startAsync();
      participantService.awaitRunning();
      _participants.put(containerId, participantService);
      _states.put(containerId, ContainerState.CONNECTED);
      started++;
      LOG.info(String.format("START: %d %d %d %d", allocated, started, stopped, deallocated));
      SettableFuture<Boolean> future = SettableFuture.create();
      future.set(true);
      return future;
    }

    @Override
    public ListenableFuture<Boolean> stopContainer(ContainerId containerId) {
      ParticipantService participant = _participants.get(containerId);
      participant.stopAsync();
      participant.awaitTerminated();
      _states.put(containerId, ContainerState.HALTED);
      stopped++;
      LOG.info(String.format("STOP: %d %d %d %d", allocated, started, stopped, deallocated));
      SettableFuture<Boolean> future = SettableFuture.create();
      future.set(true);
      return future;
    }

    @Override
    public TargetProviderResponse evaluateExistingContainers(Cluster cluster,
        ResourceId resourceId, Collection<Participant> participants) {
      TargetProviderResponse response = new TargetProviderResponse();
      // ask for one container at a time
      List<ContainerSpec> containersToAcquire = Lists.newArrayList();
      boolean asked = false;
      if (_askCount < MAX_PARTICIPANTS) {
        containersToAcquire.add(new ContainerSpec(ParticipantId.from("container" + _askCount)));
        asked = true;
      }
      List<Participant> containersToStart = Lists.newArrayList();
      List<Participant> containersToStop = Lists.newArrayList();
      List<Participant> containersToRelease = Lists.newArrayList();
      int stopCount = 0;
      for (Participant participant : participants) {
        ContainerConfig containerConfig = participant.getContainerConfig();
        if (containerConfig != null && containerConfig.getState() != null) {
          ContainerState state = containerConfig.getState();
          switch (state) {
          case ACQUIRED:
            // acquired containers are ready to start
            containersToStart.add(participant);
            break;
          case CONNECTED:
            // stop at most one active at a time, wait for everything to be up first
            if (stopCount < 1 && _askCount >= MAX_PARTICIPANTS) {
              containersToStop.add(participant);
              stopCount++;
            }
            break;
          case HALTED:
            // halted containers can be released
            containersToRelease.add(participant);
            break;
          default:
            break;
          }
          ContainerId containerId = containerConfig.getId();
          if (containerId != null) {
            _containerParticipants.put(containerId, participant.getId());
            _states.put(containerId, state);
          }
        }
      }
      // update acquire request count
      if (asked) {
        _askCount++;
      }
      // set the response
      response.setContainersToAcquire(containersToAcquire);
      response.setContainersToStart(containersToStart);
      response.setContainersToStop(containersToStop);
      response.setContainersToRelease(containersToRelease);
      return response;
    }

    @Override
    public ContainerProvider getContainerProvider() {
      return this;
    }

    @Override
    public TargetProvider getTargetProvider() {
      return this;
    }
  }
}
