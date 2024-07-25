package org.apache.helix.constants;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class InstanceConstants {
  public static final String INSTANCE_NOT_DISABLED = "INSTANCE_NOT_DISABLED";

  /**
   * The set contains the InstanceOperations that are allowed to be assigned replicas by the rebalancer.
   */
  public static final Set<InstanceOperation> ASSIGNABLE_INSTANCE_OPERATIONS =
      ImmutableSet.of(InstanceOperation.ENABLE, InstanceOperation.DISABLE);


  /**
   * The set contains the InstanceOperations that are overridden when the deprecated HELIX_ENABLED
   * field is set to false. This will maintain backwards compatibility with the deprecated field.
   * TODO: Remove this when the deprecated HELIX_ENABLED is removed.
   */
  public static final Set<InstanceOperation> INSTANCE_DISABLED_OVERRIDABLE_OPERATIONS =
      ImmutableSet.of(InstanceOperation.ENABLE, InstanceOperation.EVACUATE);


  /**
   * The set of InstanceOperations that are not allowed to be populated in the RoutingTableProvider.
   */
  public static final Set<InstanceOperation> UNROUTABLE_INSTANCE_OPERATIONS =
      ImmutableSet.of(InstanceOperation.SWAP_IN, InstanceOperation.UNKNOWN);

  @Deprecated
  public enum InstanceDisabledType {
    CLOUD_EVENT,
    USER_OPERATION,
    DEFAULT_INSTANCE_DISABLE_TYPE
  }

  public enum InstanceOperationSource {
    ADMIN(0), USER(1), AUTOMATION(2), DEFAULT(3);

    private final int _priority;

    InstanceOperationSource(int priority) {
      _priority = priority;
    }

    public int getPriority() {
      return _priority;
    }

    /**
     * Convert from InstanceDisabledType to InstanceOperationTrigger
     *
     * @param disabledType InstanceDisabledType
     * @return InstanceOperationTrigger
     */
    public static InstanceOperationSource instanceDisabledTypeToInstanceOperationSource(
        InstanceDisabledType disabledType) {
      switch (disabledType) {
        case CLOUD_EVENT:
          return InstanceOperationSource.AUTOMATION;
        case USER_OPERATION:
          return InstanceOperationSource.USER;
        default:
          return InstanceOperationSource.DEFAULT;
      }
    }
  }

  public enum InstanceOperation {
    /**
     * Behavior: Replicas will be assigned to the node and will receive upward state transitions if
     * for new assignments and downward state transitions if replicas are being moved elsewhere.
     * Final State: The node will have replicas assigned to it and will be considered for future assignment.
     */
    ENABLE,
    /**
     * Behavior: All replicas on the node will be set to OFFLINE.
     * Final State: The node will have all replicas in the OFFLINE state and can't take new assignment.
     */
    DISABLE,
    /**
     * Behavior: All replicas will be moved off the node, after a replacement has been bootstrapped
     * in another node in the cluster.
     * Final State: The node will not contain any replicas and will not be considered for *NEW* assignment.
     */
    EVACUATE,
    /**
     * Behavior: Node will have all replicas on its corresponding(same logicalId) swap-out node bootstrapped
     * (ERROR and OFFLINE replicas on swap-out node will not be bootstrapped) to the same states if the StateModelDef allows.
     * This node will be excluded from the RoutingTableProvider.
     * Final State: This node will be a mirror the swap-out node, will not be considered for assignment, and will not be populated
     * in the RoutingTableProvider.
     */
    SWAP_IN,
    /**
     * Behavior: Node will have all of its replicas dropped immediately and will be removed from the RoutingTableProvider.
     * Final State: Node will not hold replicas, be considered for assignment, or be populated in the RoutingTableProvider.
     */
    UNKNOWN
  }

  public static final String ALL_RESOURCES_DISABLED_PARTITION_KEY = "ALL_RESOURCES";
}
