package org.apache.helix.api.exceptions;

public class InstanceConfigMismatchException extends IllegalArgumentException {
  public InstanceConfigMismatchException(String message) {
    super(message);
  }
}