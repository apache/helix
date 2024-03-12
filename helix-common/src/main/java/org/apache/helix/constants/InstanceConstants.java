package org.apache.helix.constants;

import java.util.Set;

public class InstanceConstants {
  public static final String INSTANCE_NOT_DISABLED = "INSTANCE_NOT_DISABLED";

  // This means that the instance can have replicas assigned to it by the rebalancer.
  // It does not necessarily mean that the replicas will be in ONLINE state.
  public static final Set<InstanceOperation> ASSIGNABLE_INSTANCE_OPERATIONS =
      Set.of(InstanceOperation.ENABLE, InstanceOperation.DISABLE);

  // This means that the instance can host partitions that are not OFFLINE. It does not necessarily
  // mean that the instance can take new assignment.
  public static final Set<InstanceOperation> INSTANCE_OPERATIONS_HOSTING_ONLINE_REPLICAS =
      Set.of(InstanceOperation.ENABLE, InstanceOperation.EVACUATE);

  // For backwards compatibility with HELIX_ENABLED, we consider the following InstanceOperations
  // as DISABLED when HELIX_ENABLED is set to false.
  public static final Set<InstanceOperation> INSTANCE_DISABLED_OVERRIDABLE_OPERATIONS =
      Set.of(InstanceOperation.ENABLE, InstanceOperation.DISABLE, InstanceOperation.EVACUATE);

  public enum InstanceDisabledType {
    CLOUD_EVENT,
    USER_OPERATION,
    DEFAULT_INSTANCE_DISABLE_TYPE
  }

  public enum InstanceOperation {
    ENABLE, // ENABLE: Node can host online replicas and is assignable
    DISABLE, // DISABLE: Node cannot host online replicas but can be assigned replicas
    EVACUATE, // EVACUATE: Node can host online replicas but cannot be assigned new replicas
    SWAP_IN,  // SWAP_IN: Node is being swapped in and can host online replicas but is not assigned replicas
    // by the rebalancer. The mirroring is handled outside the rebalancer. SWAP_IN nodes are not populated in the
    // RoutingTableProvider.
    UNKNOWN // UNKNOWN: Node cannot host online replicas, is not assignable, and is not populated in the RoutingTableProvider.
  }
}
