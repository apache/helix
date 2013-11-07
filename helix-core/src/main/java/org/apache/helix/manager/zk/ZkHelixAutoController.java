package org.apache.helix.manager.zk;

import org.apache.helix.ClusterMessagingService;
import org.apache.helix.HelixAutoController;
import org.apache.helix.HelixConnection;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceInfoProvider;
import org.apache.helix.PreConnectCallback;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.log4j.Logger;

public class ZkHelixAutoController implements HelixAutoController {
  private static Logger LOG = Logger.getLogger(ZkHelixAutoController.class);

  final ZkHelixConnection _connection;
  final ClusterId _clusterId;
  final ControllerId _controllerId;
  final ZkHelixParticipant _participant;
  final ZkHelixController _controller;

  public ZkHelixAutoController(ZkHelixConnection connection, ClusterId clusterId,
      ControllerId controllerId) {
    _connection = connection;
    _clusterId = clusterId;
    _controllerId = controllerId;

    _participant =
        new ZkHelixParticipant(connection, clusterId, ParticipantId.from(controllerId.stringify()));
    _controller = new ZkHelixController(connection, clusterId, controllerId);
  }

  @Override
  public HelixConnection getConnection() {
    return _connection;
  }

  @Override
  public ClusterId getClusterId() {
    return _clusterId;
  }

  @Override
  public Id getId() {
    return getControllerId();
  }

  @Override
  public InstanceType getType() {
    return InstanceType.CONTROLLER_PARTICIPANT;
  }

  @Override
  public ClusterMessagingService getMessagingService() {
    return _participant.getMessagingService();
  }

  @Override
  public void startAsync() {
    _connection.addConnectionStateListener(this);
    onConnected();
  }

  @Override
  public void stopAsync() {
    _connection.removeConnectionStateListener(this);
    onDisconnecting();
  }

  @Override
  public void onConnected() {
    _controller.reset();
    _participant.reset();

    _participant.init();
    _controller.init();
  }

  @Override
  public void onDisconnecting() {
    LOG.info("disconnecting " + _controllerId + "(" + getType() + ") from " + _clusterId);
    _controller.onDisconnecting();
    _participant.onDisconnecting();
  }

  @Override
  public ControllerId getControllerId() {
    return _controllerId;
  }

  @Override
  public StateMachineEngine getStateMachineEngine() {
    return _participant.getStateMachineEngine();
  }

  @Override
  public void addPreConnectCallback(PreConnectCallback callback) {
    _participant.addPreConnectCallback(callback);
  }

  @Override
  public void setLiveInstanceInfoProvider(LiveInstanceInfoProvider liveInstanceInfoProvider) {
    _participant.setLiveInstanceInfoProvider(liveInstanceInfoProvider);
  }

  @Override
  public boolean isLeader() {
    return _controller.isLeader();
  }

}
