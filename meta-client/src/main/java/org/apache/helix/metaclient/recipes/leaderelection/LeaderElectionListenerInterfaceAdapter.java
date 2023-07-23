package org.apache.helix.metaclient.recipes.leaderelection;

import org.apache.helix.metaclient.api.DataChangeListener;

import static org.apache.helix.metaclient.recipes.leaderelection.LeaderElectionListenerInterface.ChangeType.*;


public class LeaderElectionListenerInterfaceAdapter implements DataChangeListener {
  private final LeaderElectionListenerInterface _leaderElectionListener;

  public LeaderElectionListenerInterfaceAdapter(LeaderElectionListenerInterface leaderElectionListener) {
    _leaderElectionListener = leaderElectionListener;
  }

  @Override
  public void handleDataChange(String key, Object data, ChangeType changeType) throws Exception {
    switch (changeType) {
      case  ENTRY_CREATED:
        String newLeader = ((LeaderInfo) data).getLeaderName();
        _leaderElectionListener.onLeadershipChange(key, LEADER_ACQUIRED, newLeader);
        break;
      case ENTRY_DELETED:
        _leaderElectionListener.onLeadershipChange(key, LEADER_LOST, "");
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
}
