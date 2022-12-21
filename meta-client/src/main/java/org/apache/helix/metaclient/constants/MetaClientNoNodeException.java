package org.apache.helix.metaclient.constants;

public final class MetaClientNoNodeException extends MetaClientException {
  public MetaClientNoNodeException() {
    super();
  }

  public MetaClientNoNodeException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaClientNoNodeException(String message) {
    super(message);
  }

  public MetaClientNoNodeException(Throwable cause) {
    super(cause);
  }

}