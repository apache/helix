package org.apache.helix.metaclient.recipes.leaderelection;

import org.apache.helix.metaclient.api.ConnectStateChangeListener;
import org.apache.helix.metaclient.api.DataChangeListener;
import org.apache.helix.metaclient.api.MetaClientInterface;

import static org.apache.helix.metaclient.recipes.leaderelection.LeaderElectionListenerInterface.ChangeType.*;


public class LeaderElectionListenerInterfaceAdapter implements DataChangeListener, ConnectStateChangeListener {
  private String _leaderPath;
  private final LeaderElectionListenerInterface _leaderElectionListener;

  public LeaderElectionListenerInterfaceAdapter(String leaderPath, LeaderElectionListenerInterface leaderElectionListener) {
    _leaderPath = leaderPath;
    _leaderElectionListener = leaderElectionListener;
  }

  @Override
  public void handleDataChange(String key, Object data, ChangeType changeType) throws Exception {
    switch (changeType) {
      case  ENTRY_CREATED:
      case ENTRY_UPDATE:
        String newLeader = ((LeaderInfo) data).getLeaderName();
        _leaderElectionListener.onLeadershipChange(_leaderPath, LEADER_ACQUIRED, newLeader);
        break;
      case ENTRY_DELETED:
        _leaderElectionListener.onLeadershipChange(_leaderPath, LEADER_LOST, "");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LeaderElectionListenerInterfaceAdapter that = (LeaderElectionListenerInterfaceAdapter) o;
    return _leaderElectionListener.equals(that._leaderElectionListener);
  }

  @Override
  public int hashCode() {
    return _leaderElectionListener.hashCode();
  }

  @Override
  public void handleConnectStateChanged(MetaClientInterface.ConnectState prevState,
      MetaClientInterface.ConnectState currentState) throws Exception {
    if (currentState == MetaClientInterface.ConnectState.DISCONNECTED) {
      // when disconnected, notify leader lost even though the ephmeral node is not gone until expire
      // Leader election client will touch the node if reconnect before expire
      _leaderElectionListener.onLeadershipChange(_leaderPath, LEADER_LOST, "");
    }

  }

  @Override
  public void handleConnectionEstablishmentError(Throwable error) throws Exception {

  }
}
