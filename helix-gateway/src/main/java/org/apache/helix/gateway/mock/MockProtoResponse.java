package org.apache.helix.gateway.mock;

public class MockProtoResponse {

  private String _messageId;

  public MockProtoResponse(String messageId) {
    System.out.println("Finished process of message : " + messageId);
    _messageId = messageId;
  }

  public String getMessageId() {
    return _messageId;
  }
}
