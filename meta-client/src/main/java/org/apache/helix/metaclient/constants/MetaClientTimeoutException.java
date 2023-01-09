package org.apache.helix.metaclient.constants;

public final class MetaClientTimeoutException extends MetaClientException {
  public MetaClientTimeoutException() {
    super();
  }

  public MetaClientTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaClientTimeoutException(String message) {
    super(message);
  }

  public MetaClientTimeoutException(Throwable cause) {
    super(cause);
  }

}
