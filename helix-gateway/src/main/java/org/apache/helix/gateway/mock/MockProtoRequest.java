package org.apache.helix.gateway.mock;

import org.apache.helix.gateway.constant.MessageType;

public class MockProtoRequest {

  private String _messageId;
  private String _instanceName;

  private MessageType _messageType;
  private String _resourceName;
  private String _shardName;

  private String _fromState;
  private String _toState;

  public MockProtoRequest(MessageType messageType, String resourceName, String shardName,
      String instanceName, String messageId, String fromState, String toState) {
    System.out.println(
        messageType + " | " + shardName + " | " + resourceName + " | " + instanceName + " | "
            + messageId + " | " + fromState + " | " + toState);
    _messageId = messageId;
    _instanceName = instanceName;
    _messageType = messageType;
    _resourceName = resourceName;
    _shardName = shardName;
  }

  public MessageType getMessageType() {
    return _messageType;
  }

  public String getResourceName() {
    return _resourceName;
  }

  public String getShardName() {
    return _shardName;
  }

  public String getFromState() {
    return _fromState;
  }

  public String getToState() {
    return _toState;
  }

  public String getMessageId() {
    return _messageId;
  }

  public String getInstanceName() {
    return _instanceName;
  }
}
