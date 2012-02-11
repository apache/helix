package com.linkedin.helix.participant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.HelixConstants.ChangeType;
import com.linkedin.helix.HelixConstants.StateModelToken;
import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixAgent;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.agent.zk.ZKDataAccessor;
import com.linkedin.helix.agent.zk.ZNRecordSerializer;
import com.linkedin.helix.agent.zk.ZkClient;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;

public class ParticipantCodeBuilder
{
  private static Logger LOG = Logger.getLogger(ParticipantCodeBuilder.class);
  private static String PARTICIPANT_LEADER = "PARTICIPANT_LEADER";

  private ParticipantLeaderCallback _callback;
  private List<ChangeType> _notificationTypes;
  private String _resGroupName;
  private final HelixAgent _manager;
  private final String _zkAddr;

  public ParticipantCodeBuilder(HelixAgent manager, String zkAddr)
  {
    _manager = manager;
    _zkAddr = zkAddr;
  }

  public ParticipantCodeBuilder invoke(ParticipantLeaderCallback callback)
  {
    _callback = callback;
    return this;
  }

  public ParticipantCodeBuilder on(ChangeType... notificationTypes)
  {
    _notificationTypes = Arrays.asList(notificationTypes);
    return this;
  }

  public ParticipantCodeBuilder usingLeaderStandbyModel(String resGroupName)
  {
    _resGroupName = PARTICIPANT_LEADER + "_" + resGroupName;
    return this;
  }

  public void build() throws Exception
  {
    if (_callback == null || _notificationTypes == null
        || _notificationTypes.size() == 0
        || _resGroupName == null)
    {
      throw new IllegalArgumentException("Require callback | notificationTypes | resourceGroupName");
    }

    ParticipantLeaderStateModelFactory stateModelFty
      = new ParticipantLeaderStateModelFactory(_callback, _notificationTypes);

    StateMachineEngine stateMach = _manager.getStateMachineEngine();
    stateMach.registerStateModelFactory("LeaderStandby", _resGroupName, stateModelFty);

    // manually add ideal state for participant leader using LeaderStandby model
    ZkClient zkClient = new ZkClient(_zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    DataAccessor accessor = new ZKDataAccessor(_manager.getClusterName(), zkClient);

    IdealState idealState = new IdealState(_resGroupName);
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    idealState.setReplicas(StateModelToken.ANY_LIVEINSTANCE.toString());
    idealState.setNumPartitions(1);
    idealState.setStateModelDefRef("LeaderStandby");
    List<String> prefList = new ArrayList<String>(Arrays.asList(StateModelToken.ANY_LIVEINSTANCE.toString()));
    idealState.getRecord().setListField(_resGroupName + "_0", prefList);

    List<String> idealStates = accessor.getChildNames(PropertyType.IDEALSTATES);
    while (idealStates == null || !idealStates.contains(_resGroupName))
    {   
      accessor.setProperty(PropertyType.IDEALSTATES, idealState, _resGroupName);
      idealStates = accessor.getChildNames(PropertyType.IDEALSTATES);
    }
    
    LOG.debug("Set idealState for participantLeader:" + _resGroupName 
                + ", idealState:" + idealState);
  }
}
