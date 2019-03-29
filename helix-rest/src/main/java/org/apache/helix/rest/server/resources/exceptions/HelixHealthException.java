package org.apache.helix.rest.server.resources.exceptions;

public class HelixHealthException extends RuntimeException {

  public HelixHealthException(String message) {
    super(message);
  }

  public HelixHealthException(Throwable cause) {
    super(cause);
  }

  public HelixHealthException(String message, Throwable cause) {
    super(message, cause);
  }
}
