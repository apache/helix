package org.apache.helix.constants;

public class InstanceConstants {
  public static final String INSTANCE_NOT_DISABLED = "INSTANCE_NOT_DISABLED";

  public enum InstanceDisabledType {
    CLOUD_EVENT,
    USER_OPERATION,
    DEFAULT_INSTANCE_DISABLE_TYPE
  }

  public enum InstanceOperation {
    EVACUATE,
    SWAP_IN,
    SWAP_OUT,
    ENABLE,
    DISABLE;
  }
}
