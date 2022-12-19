package org.apache.helix.rest.server;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementInstanceInfo;
import org.apache.helix.rest.clusterMaintenanceService.api.OperationInterface;
import org.apache.helix.rest.common.RestSnapShotSimpleImpl;
import org.apache.helix.rest.common.datamodel.RestSnapShot;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.util.InstanceValidationUtil;


public class TestOperationImpl implements OperationInterface {

  @Override
  public MaintenanceManagementInstanceInfo operationCheckForTakeSingleInstance(String instanceName, Map<String, String> operationConfig, RestSnapShot sn) {
    Map<String, Boolean> isInstanceOnHoldCache = new HashMap<>();
    for (Map.Entry<String, String> entry : operationConfig.entrySet()) {
      isInstanceOnHoldCache.put(entry.getKey(), Boolean.parseBoolean(entry.getValue()));
    }
    try {
      String unHealthyPartition =
          siblingNodesActiveReplicaCheck(sn, instanceName, isInstanceOnHoldCache);
      if (unHealthyPartition == null) {
        return new MaintenanceManagementInstanceInfo(
            MaintenanceManagementInstanceInfo.OperationalStatus.SUCCESS);
      } else {
        return new MaintenanceManagementInstanceInfo(
            MaintenanceManagementInstanceInfo.OperationalStatus.FAILURE,
            Collections.singletonList(unHealthyPartition));
      }
    } catch (Exception ex) {
      return new MaintenanceManagementInstanceInfo(
          MaintenanceManagementInstanceInfo.OperationalStatus.FAILURE,
          Collections.singletonList(ex.getMessage()));
    }
  }

  @Override
  public MaintenanceManagementInstanceInfo operationCheckForFreeSingleInstance(String instanceName, Map<String, String> operationConfig, RestSnapShot sn) {
    return null;
  }

  @Override
  public Map<String, MaintenanceManagementInstanceInfo> operationCheckForTakeInstances(Collection<String> instances, Map<String, String> operationConfig, RestSnapShot sn) {
    return null;
  }

  @Override
  public Map<String, MaintenanceManagementInstanceInfo> operationCheckForFreeInstances(Collection<String> instances, Map<String, String> operationConfig, RestSnapShot sn) {
    return null;
  }

  @Override
  public MaintenanceManagementInstanceInfo operationExecForTakeSingleInstance(String instanceName,
      Map<String, String> operationConfig, RestSnapShot sn) {
    return new MaintenanceManagementInstanceInfo(
        MaintenanceManagementInstanceInfo.OperationalStatus.SUCCESS,
        "DummyTakeOperationResult");
  }

  @Override
  public MaintenanceManagementInstanceInfo operationExecForFreeSingleInstance(String instanceName,
      Map<String, String> operationConfig, RestSnapShot sn) {
    return new MaintenanceManagementInstanceInfo(
        MaintenanceManagementInstanceInfo.OperationalStatus.SUCCESS,
        "DummyFreeOperationResult");
  }

  @Override
  public Map<String, MaintenanceManagementInstanceInfo> operationExecForTakeInstances(Collection<String> instances, Map<String, String> operationConfig, RestSnapShot sn) {
    return null;
  }

  @Override
  public Map<String, MaintenanceManagementInstanceInfo> operationExecForFreeInstances(Collection<String> instances, Map<String, String> operationConfig, RestSnapShot sn) {
    return null;
  }

  public String siblingNodesActiveReplicaCheck(RestSnapShot snapShot, String instanceName,
      Map<String, Boolean> isInstanceOnHoldCache) throws HelixException {
    String clusterName = snapShot.getClusterName();
    if (!(snapShot instanceof RestSnapShotSimpleImpl)) {
      throw new HelixException("Passed in Snapshot is not an instance of RestSnapShotSimpleImpl");
    } RestSnapShotSimpleImpl restSnapShotSimple = (RestSnapShotSimpleImpl) snapShot;

    PropertyKey.Builder propertyKeyBuilder = new PropertyKey.Builder(clusterName);
    List<String> resources = restSnapShotSimple.getChildNames(propertyKeyBuilder.idealStates());

    for (String resourceName : resources) {
      IdealState idealState =
          restSnapShotSimple.getProperty(propertyKeyBuilder.idealStates(resourceName));
      if (idealState == null || !idealState.isEnabled() || !idealState.isValid()
          || TaskConstants.STATE_MODEL_NAME.equals(idealState.getStateModelDefRef())) {
        continue;
      }
      ExternalView externalView =
          restSnapShotSimple.getProperty(propertyKeyBuilder.externalView(resourceName));
      if (externalView == null) {
        throw new HelixException(
            String.format("Resource %s does not have external view!", resourceName));
      }
      // Get the minActiveReplicas constraint for the resource
      int minActiveReplicas = externalView.getMinActiveReplicas();
      if (minActiveReplicas == -1) {
        continue;
      }
      String stateModeDef = externalView.getStateModelDefRef();
      StateModelDefinition stateModelDefinition =
          restSnapShotSimple.getProperty(propertyKeyBuilder.stateModelDef(stateModeDef));
      Set<String> unhealthyStates = new HashSet<>(InstanceValidationUtil.UNHEALTHY_STATES);
      if (stateModelDefinition != null) {
        unhealthyStates.add(stateModelDefinition.getInitialState());
      }
      for (String partition : externalView.getPartitionSet()) {
        Map<String, String> stateByInstanceMap = externalView.getStateMap(partition);
        // found the resource hosted on the instance
        if (stateByInstanceMap.containsKey(instanceName)) {
          int numHealthySiblings = 0;
          for (Map.Entry<String, String> entry : stateByInstanceMap.entrySet()) {
            if (!entry.getKey().equals(instanceName) && !unhealthyStates.contains(entry.getValue())
                && !isInstanceOnHoldCache.get(entry.getKey())) {
              numHealthySiblings++;
            }
          }
          if (numHealthySiblings < minActiveReplicas) {
            return partition;
          }
        }
      }
    }

    return null;
  }
}

