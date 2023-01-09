package org.apache.helix.metaclient.constants;

public final class MetaClientInterruptException extends MetaClientException {
  public MetaClientInterruptException() {
    super();
  }

  public MetaClientInterruptException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaClientInterruptException(String message) {
    super(message);
  }

  public MetaClientInterruptException(Throwable cause) {
    super(cause);
  }
}
