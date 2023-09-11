package org.apache.helix.constants;

public class InstanceConstants {
  public static final String INSTANCE_NOT_DISABLED = "INSTANCE_NOT_DISABLED";

  public enum InstanceDisabledType {
    CLOUD_EVENT,
    USER_OPERATION,
    DEFAULT_INSTANCE_DISABLE_TYPE
  }

  public enum InstanceOperation {
    EVACUATE, // Node will be removed after a period of time
    SWAP_IN,  // New node joining for swap operation
    SWAP_OUT // Existing Node to be removed for swap operation
  }
}
