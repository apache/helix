package org.apache.helix.rest.clusterMaintenanceService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

;

public class MaintenanceManagementService {

  /**
   * Perform health check and maintenance operation check and execution for a instance in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForTakeSingleInstance and
   * OperationAbstractClass.operationExecForTakeSingleInstance.
   *
   * @param clusterId          The cluster id
   * @param instanceName       The instance name
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public MaintenanceManagementInstanceInfo takeInstance(String clusterId, String instanceName,
      List<String> healthChecks, Map<String, String> healthCheckConfig, List<String> operations,
      Map<String, String> operationConfig, boolean performOperation) throws IOException {
    return null;
  }

  /**
   * Perform health check and maintenance operation check and execution for a list of instances in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForTakeInstances and
   * OperationAbstractClass.operationExecForTakeInstances.
   *
   * @param clusterId          The cluster id
   * @param instances          A list of instances
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return A list of MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public Map<String, MaintenanceManagementInstanceInfo> takeInstances(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig,
      List<String> operations, Map<String, String> operationConfig, boolean performOperation)
      throws IOException {
    return null;
  }

  /**
   * Perform health check and maintenance operation check and execution for a instance in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForFreeSingleInstance and
   * OperationAbstractClass.operationExecForFreeSingleInstance.
   *
   * @param clusterId          The cluster id
   * @param instanceName       The instance name
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public MaintenanceManagementInstanceInfo freeInstance(String clusterId, String instanceName,
      List<String> healthChecks, Map<String, String> healthCheckConfig, List<String> operations,
      Map<String, String> operationConfig, boolean performOperation) throws IOException {
    return null;
  }

  /**
   * Perform health check and maintenance operation check and execution for a list of instances in
   * one cluster.
   * User need to implement OperationAbstractClass for customer operation check & execution.
   * It will invoke OperationAbstractClass.operationCheckForFreeInstances and
   * OperationAbstractClass.operationExecForFreeInstances.
   *
   * @param clusterId          The cluster id
   * @param instances          A list of instances
   * @param healthChecks       A list of healthChecks to perform
   * @param healthCheckConfig The input for health Checks
   * @param operations         A list of operation checks or operations to execute
   * @param performOperation   If this param is set to false, the function will only do a dry run
   * @return A list of MaintenanceManagementInstanceInfo
   * @throws IOException in case of network failure
   */
  public Map<String, MaintenanceManagementInstanceInfo> freeInstances(String clusterId,
      List<String> instances, List<String> healthChecks, Map<String, String> healthCheckConfig,
      List<String> operations, Map<String, String> operationConfig, boolean performOperation)
      throws IOException {
    return null;
  }
}
