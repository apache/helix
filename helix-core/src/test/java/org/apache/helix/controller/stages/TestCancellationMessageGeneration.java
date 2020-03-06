package org.apache.helix.controller.stages;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * This test checks the cancellation message generation when currentState=null and desiredState=DROPPED
 */
public class TestCancellationMessageGeneration extends MessageGenerationPhase {
  private static final String TEST_CLUSTER = "testCluster";
  private static final String TEST_RESOURCE = "resource0";
  private static final String TEST_INSTANCE = "instance0";
  private static final String TEST_PARTITION = "partition0";

  @Test
  public void TestOFFLINEToDROPPED() throws Exception {

    ClusterEvent event = new ClusterEvent(TEST_CLUSTER, ClusterEventType.Unknown);


    // Set current state to event
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);
    Partition partition = mock(Partition.class);
    when(partition.getPartitionName()).thenReturn(TEST_PARTITION);
    when(currentStateOutput.getCurrentState(TEST_RESOURCE, partition, TEST_INSTANCE)).thenReturn(null);
    Message message = mock(Message.class);
    when(message.getFromState()).thenReturn("OFFLINE");
    when(message.getToState()).thenReturn("SLAVE");
    when(currentStateOutput.getPendingMessage(TEST_RESOURCE, partition, TEST_INSTANCE)).thenReturn(message);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    // Set helix manager to event
    event.addAttribute(AttributeName.helixmanager.name(), mock(HelixManager.class));

    // Set controller data provider to event
    BaseControllerDataProvider cache = mock(BaseControllerDataProvider.class);
    StateModelDefinition stateModelDefinition = new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    when(cache.getStateModelDef(TaskConstants.STATE_MODEL_NAME)).thenReturn(stateModelDefinition);
    Map<String, LiveInstance> liveInstances= mock(Map.class);
    LiveInstance mockLiveInstance = mock(LiveInstance.class);
    when(mockLiveInstance.getInstanceName()).thenReturn(TEST_INSTANCE);
    when(mockLiveInstance.getEphemeralOwner()).thenReturn("TEST");
    when(liveInstances.values()).thenReturn(Arrays.asList(mockLiveInstance));
    when(cache.getLiveInstances()).thenReturn(liveInstances);
    ClusterConfig clusterConfig = mock(ClusterConfig.class);
    when(cache.getClusterConfig()).thenReturn(clusterConfig);
    when(clusterConfig.isStateTransitionCancelEnabled()).thenReturn(true);
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);


    // Set resources to rebalance to event
    Map<String, Resource> resourceMap = new HashMap<>();
    Resource resource = mock(Resource.class);
    when(resource.getResourceName()).thenReturn(TEST_RESOURCE);
    List<Partition> partitions = Arrays.asList(partition);
    when(resource.getPartitions()).thenReturn(partitions);
    when(resource.getStateModelDefRef()).thenReturn(TaskConstants.STATE_MODEL_NAME);
    resourceMap.put(TEST_RESOURCE, resource);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);

    // set up resource state map
    ResourcesStateMap resourcesStateMap = new ResourcesStateMap();
    PartitionStateMap partitionStateMap = new PartitionStateMap(TEST_RESOURCE);
    Map<Partition, Map<String, String>> stateMap = partitionStateMap.getStateMap();
    Map<String, String> instanceStateMap = new HashMap<>();
    instanceStateMap.put(TEST_INSTANCE, HelixDefinedState.DROPPED.name());
    stateMap.put(partition, instanceStateMap);
    resourcesStateMap.setState(TEST_RESOURCE, partition, instanceStateMap);

    processEvent(event, resourcesStateMap);
    MessageOutput output = event.getAttribute(AttributeName.MESSAGES_ALL.name());
    Assert.assertEquals(output.getMessages(TEST_RESOURCE, partition).size(), 1);
  }
}
