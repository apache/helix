package org.apache.helix.metaclient.constants;

public class MetaClientBadVersionException extends MetaClientException {
  public MetaClientBadVersionException() {
    super();
  }

  public MetaClientBadVersionException(String message, Throwable cause) {
    super(message, cause);
  }

  public MetaClientBadVersionException(String message) {
    super(message);
  }

  public MetaClientBadVersionException(Throwable cause) {
    super(cause);
  }

}
