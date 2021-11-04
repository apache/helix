package org.apache.helix.rest.common.datamodel;


/* This Snapshot can extend Snapshot from common/core module
 * once there is more generic snapshot.
 */
public class RestSnapShot {
  /* An Snapshot object should contain all the Helix related info that an implementation of
   * OperationAbstractClass would need.
   */

  // TODO: Define a Enum class for all Helix info types like ExternalView, InstanceConfig etc. An
  // implementation of OperationAbstractClass will need to define what are the types needed.

  // TODO: Support hierarchical Snapshot type for other services besides cluster MaintenanceService.
}