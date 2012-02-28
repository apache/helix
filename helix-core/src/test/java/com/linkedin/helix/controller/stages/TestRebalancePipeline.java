package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.ClusterMessagingService;
import com.linkedin.helix.ConfigChangeListener;
import com.linkedin.helix.ControllerChangeListener;
import com.linkedin.helix.CurrentStateChangeListener;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.ExternalViewChangeListener;
import com.linkedin.helix.HealthStateChangeListener;
import com.linkedin.helix.HelixAdmin;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.IdealStateChangeListener;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.LiveInstanceChangeListener;
import com.linkedin.helix.MessageListener;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.ZkUnitTestBase;
import com.linkedin.helix.controller.pipeline.Pipeline;
import com.linkedin.helix.healthcheck.ParticipantHealthReportCollector;
import com.linkedin.helix.manager.zk.ZKDataAccessor;
import com.linkedin.helix.model.CurrentState;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Partition;
import com.linkedin.helix.model.StateModelDefinition;
import com.linkedin.helix.participant.StateMachineEngine;
import com.linkedin.helix.store.PropertyStore;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class TestRebalancePipeline extends ZkUnitTestBase
{
  private static final Logger LOG =
      Logger.getLogger(TestRebalancePipeline.class.getName());
  final String _className = getShortClassName();
  HelixManager _manager;
  DataAccessor _accessor;
  ClusterEvent _event;

  class MockClusterManager implements HelixManager
  {
    DataAccessor _accessor;
    String _clusterName;
    String _sessionId;

    public MockClusterManager(String clusterName, DataAccessor accessor)
    {
      _clusterName = clusterName;
      _accessor = accessor;
      _sessionId = "session_" + clusterName;
    }

    @Override
    public void connect() throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean isConnected()
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public void disconnect()
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addIdealStateChangeListener(IdealStateChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addLiveInstanceChangeListener(LiveInstanceChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addConfigChangeListener(ConfigChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addMessageListener(MessageListener listener, String instanceName) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addCurrentStateChangeListener(CurrentStateChangeListener listener,
                                              String instanceName,
                                              String sessionId) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public void addExternalViewChangeListener(ExternalViewChangeListener listener) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean removeListener(Object listener)
    {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public DataAccessor getDataAccessor()
    {
      return _accessor;
    }

    @Override
    public String getClusterName()
    {
      return _clusterName;
    }

    @Override
    public String getInstanceName()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getSessionId()
    {
      return _sessionId;
    }

    @Override
    public long getLastNotificationTime()
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void addControllerListener(ControllerChangeListener listener)
    {
      // TODO Auto-generated method stub

    }

    @Override
    public HelixAdmin getClusterManagmentTool()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public PropertyStore<ZNRecord> getPropertyStore()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ClusterMessagingService getMessagingService()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public ParticipantHealthReportCollector getHealthReportCollector()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public InstanceType getInstanceType()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String getVersion()
    {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void addHealthStateChangeListener(HealthStateChangeListener listener,
                                             String instanceName) throws Exception
    {
      // TODO Auto-generated method stub

    }

    @Override
    public StateMachineEngine getStateMachineEngine()
    {
      // TODO Auto-generated method stub
      return null;
    }

  }

  @Test
  public void testDuplicateMsg()
  {
    String clusterName = "CLUSTER_" + _className + "_dup";
    System.out.println("START " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

    _accessor = new ZKDataAccessor(clusterName, _gZkClient);
    _manager = new MockClusterManager(clusterName, _accessor);
    _event = new ClusterEvent("testEvent");

    final String resourceName = "testResource_dup";
    String[] resourceGroups = new String[] { resourceName };
    // ideal state: node0 is SLAVE on partition_0
    // and node1 is MASTER on partition_0
    setupIdealState(new int[] { 0, 1 }, resourceGroups, 1, 2); // replica=2 means 1 master
                                                               // and 1 slave
    setupLiveInstances(new int[] { 0, 1 }, new String[] { "0", "1" });
    setupStateModel();

    // cluster data cache refresh pipeline
    Pipeline dataRefresh = new Pipeline();
    dataRefresh.addStage(new ReadClusterDataStage());

    // rebalance pipeline
    Pipeline rebalancePipeline = new Pipeline();
    rebalancePipeline.addStage(new ResourceComputationStage());
    rebalancePipeline.addStage(new CurrentStateComputationStage());
    rebalancePipeline.addStage(new BestPossibleStateCalcStage());
    rebalancePipeline.addStage(new MessageGenerationPhase());
    rebalancePipeline.addStage(new MessageSelectionStage());
    rebalancePipeline.addStage(new TaskAssignmentStage());
    // round1: set node0's currentState to SLAVE on partition_0
    // and node1's currentState to OFFLINE on partition_0
    setCurrentState("localhost_0",
                    resourceName,
                    resourceName + "_0",
                    "session_0",
                    "SLAVE");
    setCurrentState("localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "OFFLINE");

    runPipeline(_event, dataRefresh);
    runPipeline(_event, rebalancePipeline);
    MessageSelectionStageOutput msgSelOutput =
        _event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    List<Message> messages =
        msgSelOutput.getMessages(resourceName, new Partition(resourceName
            + "_0"));
    Assert.assertEquals(messages.size(),
                        1,
                        "Should output 1 message: OFFLINE-SLAVE for node1");
    Message message = messages.get(0);
    Assert.assertEquals(message.getFromState(), "OFFLINE");
    Assert.assertEquals(message.getToState(), "SLAVE");
    Assert.assertEquals(message.getTgtName(), "localhost_1");

    // round2: localhost_1 updates its currentState to SLAVE but haven't removed the
    // message yet
    // make sure controller should not send S->M message until removal is done
    setCurrentState("localhost_1",
                    resourceName,
                    resourceName + "_0",
                    "session_1",
                    "SLAVE");

    runPipeline(_event, dataRefresh);
    runPipeline(_event, rebalancePipeline);
    msgSelOutput = _event.getAttribute(AttributeName.MESSAGES_SELECTED.toString());
    messages = msgSelOutput.getMessages(resourceName, new Partition(resourceName + "_0"));
    Assert.assertEquals(messages.size(),
                        0,
                        "Should NOT output 1 message: SLAVE-MASTER for node1");

    System.out.println("END " + clusterName + " at "
        + new Date(System.currentTimeMillis()));

  }

  protected List<IdealState> setupIdealState(int[] nodes,
                                             String[] resourceGroups,
                                             int partitions,
                                             int replicas)
  {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i : nodes)
    {
      instances.add("localhost_" + i);
    }

    for (String resourceGroupName : resourceGroups)
    {
      IdealState idealState = new IdealState(resourceGroupName);
      for (int p = 0; p < partitions; p++)
      {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++)
        {
          value.add("localhost_" + (p + r + 1) % nodes.length);
        }
        idealState.getRecord().setListField(resourceGroupName + "_" + p, value);
      }

      idealState.setReplicas(Integer.toString(replicas));
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);
      _accessor.setProperty(PropertyType.IDEALSTATES, idealState, resourceGroupName);
    }
    return idealStates;
  }

  protected void setupLiveInstances(int[] liveInstances, String[] sessionIds)
  {
    for (int i = 0; i < liveInstances.length; i++)
    {
      String instance = "localhost_" + liveInstances[i];
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId("session_" + sessionIds[i]);
      liveInstance.setHelixVersion("0.0.0");
      _accessor.setProperty(PropertyType.LIVEINSTANCES, liveInstance, instance);
    }
  }

  protected void setupStateModel()
  {
    StateModelConfigGenerator generator = new StateModelConfigGenerator();
    StateModelDefinition masterSlave =
        new StateModelDefinition(generator.generateConfigForMasterSlave());
    _accessor.setProperty(PropertyType.STATEMODELDEFS, masterSlave, masterSlave.getId());
    StateModelDefinition leaderStandby =
        new StateModelDefinition(generator.generateConfigForLeaderStandby());
    _accessor.setProperty(PropertyType.STATEMODELDEFS,
                          leaderStandby,
                          leaderStandby.getId());
    StateModelDefinition onlineOffline =
        new StateModelDefinition(generator.generateConfigForOnlineOffline());
    _accessor.setProperty(PropertyType.STATEMODELDEFS,
                          onlineOffline,
                          onlineOffline.getId());
  }

  protected void setCurrentState(String instance,
                                 String resourceGroupName,
                                 String resourceKey,
                                 String sessionId,
                                 String state)
  {
    CurrentState curState = new CurrentState(resourceGroupName);
    curState.setState(resourceKey, state);
    curState.setSessionId(sessionId);
    curState.setStateModelDefRef("MasterSlave");
    _accessor.setProperty(PropertyType.CURRENTSTATES,
                          curState,
                          instance,
                          sessionId,
                          resourceGroupName);
  }

  protected void runPipeline(ClusterEvent event, Pipeline pipeline)
  {
    event.addAttribute("helixmanager", _manager);
    try
    {
      pipeline.handle(event);
      pipeline.finish();
    }
    catch (Exception e)
    {
      LOG.error("Exception while executing pipeline:" + pipeline
          + ". Will not continue to next pipeline", e);
    }
  }
}