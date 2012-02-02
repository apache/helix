package com.linkedin.helix.participant;

import java.util.List;

import org.apache.log4j.Logger;

import com.linkedin.helix.CMConstants.ChangeType;
import com.linkedin.helix.ClusterManager;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.participant.statemachine.StateModel;
import com.linkedin.helix.participant.statemachine.StateModelInfo;
import com.linkedin.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = { "LEADER", "STANDBY" })
public class ParticipantLeaderStateModel extends StateModel
{
  private static Logger LOG = Logger.getLogger(ParticipantLeaderStateModel.class);

  private final ParticipantCodeHolder _particHolder;
  private final List<ChangeType> _notificationTypes;

  public ParticipantLeaderStateModel(ParticipantLeaderCallback callback, 
                                     List<ChangeType> notificationTypes,
                                     String partitionKey)
  {
    _particHolder = new ParticipantCodeHolder(callback, partitionKey);
    _notificationTypes = notificationTypes;
  }

  @Transition(to="STANDBY",from="OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context)
  {
    LOG.info("Become STANDBY from OFFLINE");
  }

  @Transition(to="LEADER",from="STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception
  {
    LOG.info("Become LEADER from STANDBY");
    ClusterManager manager = context.getManager();
    if (manager == null)
    {
      throw new IllegalArgumentException("Require ClusterManager in notification conext");
    }
    for (ChangeType notificationType : _notificationTypes)
    {
      if (notificationType == ChangeType.LIVE_INSTANCE)
      {
        manager.addLiveInstanceChangeListener(_particHolder);
      }
      else if (notificationType == ChangeType.CONFIG)
      {
        manager.addConfigChangeListener(_particHolder);
      }
      else if (notificationType == ChangeType.EXTERNAL_VIEW)
      {
        manager.addExternalViewChangeListener(_particHolder);
      }
      else
      {
        LOG.error("Unsupport notificationType:" + notificationType.toString());
      }
    }
  }

  @Transition(to="STANDBY",from="LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context)
  {
    LOG.info("Become STANDBY from LEADER");
    ClusterManager manager = context.getManager();
    manager.removeListener(_particHolder);    
  }

  @Transition(to="OFFLINE",from="STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context)
  {
    LOG.info("Become OFFLINE from STANDBY");
  }
}
