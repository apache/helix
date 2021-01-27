package org.apache.helix.controller.stages;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MsgRecordingPerReplicaThrottleStage extends PerReplicaThrottleStage {
  private static final Logger logger =
      LoggerFactory.getLogger(MsgRecordingPerReplicaThrottleStage.class.getName());

  private List<Message> _throttledRecoveryMsg = new ArrayList<>();
  private List<Message> _throttledLoadMsg = new ArrayList<>();

  protected void applyThrottling(Resource resource,
      StateTransitionThrottleController throttleController,
      Map<Partition, Map<String, String>> currentStateMap,
      Map<Partition, Map<String, String>> bestPossibleMap,
      IdealState idealState,
      ResourceControllerDataProvider cache,
      boolean onlyDownwardLoadBalance,
      List<Message> messages,
      Map<Message, Partition> messagePartitionMap,
      Set<Message> throttledMessages,
      StateTransitionThrottleConfig.RebalanceType rebalanceType
  ) {
    Set<Message> middleThrottledMessages = new HashSet<>();
    super.applyThrottling(resource, throttleController, currentStateMap, bestPossibleMap,
        idealState, cache, onlyDownwardLoadBalance, messages, messagePartitionMap,
        middleThrottledMessages, rebalanceType);

    if (rebalanceType == StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE) {
      _throttledRecoveryMsg.addAll(middleThrottledMessages);
    } else if (rebalanceType == StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE) {
      _throttledLoadMsg.addAll(middleThrottledMessages);
    }
    throttledMessages.addAll(middleThrottledMessages);
  }

  public List<Message> getRecoveryThrottledMessages() {
    return _throttledRecoveryMsg;
  }

  public List<Message> getLoadThrottledMessages() {
    return _throttledLoadMsg;
  }
}
