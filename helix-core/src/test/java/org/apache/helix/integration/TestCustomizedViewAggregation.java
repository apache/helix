package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.Maps;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.customizedstate.CustomizedStateProvider;
import org.apache.helix.customizedstate.CustomizedStateProviderFactory;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestCustomizedViewAggregation extends ZkUnitTestBase {

  private static CustomizedStateProvider _customizedStateProvider_participant0;
  private static CustomizedStateProvider _customizedStateProvider_participant1;
  private static RoutingTableProvider _routingTableProvider;
  private static HelixManager _spectator;
  private static HelixManager _manager;
  // 1st key: customized state type, 2nd key: resource name, 3rd key: partition name, 4th key: instance name, value: state value
  // This map contains all the customized state information that is enabled for aggregation in config, including those are not listened by routing table provider
  private static Map<String, Map<String, Map<String, Map<String, String>>>> _localCustomizedView;
  // The set contains customized state types that are listened by routing table provider
  private static Set<String> _localVisibleCustomizedStateType;
  private String INSTANCE_0;
  private String INSTANCE_1;
  private final String RESOURCE_A = "TestDB0";
  private final String RESOURCE_B = "TestDB1";
  private final String PARTITION_A1 = "TestDB0_0";
  private final String PARTITION_A2 = "TestDB0_1";
  private final String PARTITION_B1 = "TestDB1_0";
  private final String PARTITION_B2 = "TestDB1_1";

  // Customized state values used for test, StatusA1 - StatusA3 are values for Customized state TypeA, etc.
  private enum CurrentStateValues {
    StatusA1, StatusA2, StatusA3, StatusB1, StatusB2, StatusB3, StatusC1, StatusC2, StatusC3
  }

  private enum CustomizedStateType {
    TYPE_A, TYPE_B, TYPE_C
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        2, // resources
        2, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    INSTANCE_0 = participants[0].getInstanceName();
    INSTANCE_1 = participants[1].getInstanceName();

    _manager = HelixManagerFactory
        .getZKHelixManager(clusterName, "admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _spectator = HelixManagerFactory
        .getZKHelixManager(clusterName, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();
    HelixDataAccessor dataAccessor = _manager.getHelixDataAccessor();

    // Initialize customized state provider
    _customizedStateProvider_participant0 = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(_manager, participants[0].getInstanceName());
    _customizedStateProvider_participant1 = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(_manager, participants[1].getInstanceName());

    // Set up aggregation config
    List<String> aggregationEnabledTypes = Arrays
        .asList(CustomizedStateType.TYPE_A.name(), CustomizedStateType.TYPE_B.name(),
            CustomizedStateType.TYPE_C.name());
    CustomizedStateConfig.Builder customizedStateConfigBuilder =
        new CustomizedStateConfig.Builder();
    customizedStateConfigBuilder.setAggregationEnabledTypes(aggregationEnabledTypes);
    dataAccessor.updateProperty(dataAccessor.keyBuilder().customizedStateConfig(),
        customizedStateConfigBuilder.build());

    _localCustomizedView = new HashMap<>();
    _localVisibleCustomizedStateType = new HashSet<>();
  }

  @AfterClass
  public void afterClass() {
    _routingTableProvider.shutdown();
    _manager.disconnect();
    _spectator.disconnect();
  }

  /**
   * Compare the customized state values between ZK and local record
   * @throws Exception thread interrupted exception
   */
  private void validateAggregationSnapshot() throws Exception {
    boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        Map<String, Map<String, RoutingTableSnapshot>> routingTableSnapshots =
            _routingTableProvider.getRoutingTableSnapshots();

        // Get customized view snapshot
        Map<String, RoutingTableSnapshot> fullCustomizedViewSnapshot =
            routingTableSnapshots.get(PropertyType.CUSTOMIZEDVIEW.name());

        for (String customizedStateType : fullCustomizedViewSnapshot.keySet()) {
          if (!_localVisibleCustomizedStateType.contains(customizedStateType)) {
            System.out.println(
                "Local record does not contain customized state type " + customizedStateType
                    + ", while it is shown in snapshot");
            return false;
          }

          // Get per customized state type snapshot
          RoutingTableSnapshot customizedViewSnapshot =
              fullCustomizedViewSnapshot.get(customizedStateType);

          // local per customized state type map
          Map<String, Map<String, Map<String, String>>> localSnapshot =
              _localCustomizedView.getOrDefault(customizedStateType, Maps.newHashMap());

          Collection<CustomizedView> customizedViews = customizedViewSnapshot.getCustomizeViews();

          // Get per resource snapshot
          for (CustomizedView resourceCustomizedView : customizedViews) {
            ZNRecord record = resourceCustomizedView.getRecord();
            Map<String, Map<String, String>> resourceStateMap = record.getMapFields();

            // Get local per resource map
            Map<String, Map<String, String>> localPerResourceCustomizedView = localSnapshot
                .getOrDefault(resourceCustomizedView.getResourceName(), Maps.newHashMap());

            // Get per partition snapshot
            for (String partitionName : resourceStateMap.keySet()) {
              Map<String, String> stateMap =
                  resourceStateMap.getOrDefault(partitionName, Maps.newTreeMap());

              // Get local per partition map
              Map<String, String> localStateMap =
                  localPerResourceCustomizedView.getOrDefault(partitionName, Maps.newTreeMap());

              for (String instanceName : stateMap.keySet()) {
                // Per instance value
                String stateMapValue = stateMap.get(instanceName);
                String localStateMapValue = localStateMap.get(instanceName);
                if (isEmptyValue(stateMapValue) && isEmptyValue(localStateMapValue)) {
                  return true;
                }
                if ((!isEmptyValue(stateMapValue) && !isEmptyValue(localStateMapValue)
                    && !stateMapValue.equals(localStateMapValue)) || (isEmptyValue(stateMapValue)
                    || isEmptyValue(localStateMapValue))) {
                  System.out.println("The customized state value is: " + stateMapValue
                      + ", it does not match local record value: " + localStateMapValue
                      + ", for instance " + instanceName + ".");
                  return false;
                }
                return true;
              }
            }
          }
        }
        return false; // There is no any customized state type enabled for aggregation set
      }
    }, 12000);

    Assert.assertTrue(result);
  }

  private boolean isEmptyValue(String value) {
    return value == null || value.equals("");
  }

  /**
   * Update the local record of customized state
   * @param instanceName the instance to be updated
   * @param customizedStateType the customized state type to be updated
   * @param resourceName the resource to be updated
   * @param partitionName the partition to be updated
   * @param customizedStateValue if update, this will be the value to update; a null value indicate delete operation
   */
  private void updateLocalCustomizedViewMap(String instanceName,
      CustomizedStateType customizedStateType, String resourceName, String partitionName,
      CurrentStateValues customizedStateValue) {
    _localCustomizedView.putIfAbsent(customizedStateType.name(), new TreeMap<>());
    Map<String, Map<String, Map<String, String>>> localPerStateType =
        _localCustomizedView.get(customizedStateType.name());
    localPerStateType.putIfAbsent(resourceName, new TreeMap<>());
    Map<String, Map<String, String>> localPerResource = localPerStateType.get(resourceName);
    localPerResource.putIfAbsent(partitionName, new TreeMap<>());
    Map<String, String> localPerPartition = localPerResource.get(partitionName);
    if (customizedStateValue == null) {
      localPerPartition.remove(instanceName);
    } else {
      localPerPartition.put(instanceName, customizedStateValue.name());
    }
  }

  /**
   * Call this method in the test for an update on customized state in both ZK and local map
   * @param instanceName the instance to be updated
   * @param customizedStateType the customized state type to be updated
   * @param resourceName the resource to be updated
   * @param partitionName the partition to be updated
   * @param customizedStateValue if update, this will be the value to update; a null value indicate delete operation
   * @throws Exception if the input instance name is not valid
   */
  private void update(String instanceName, CustomizedStateType customizedStateType,
      String resourceName, String partitionName, CurrentStateValues customizedStateValue)
      throws Exception {
    if (instanceName.equals(INSTANCE_0)) {
      _customizedStateProvider_participant0
          .updateCustomizedState(customizedStateType.name(), resourceName, partitionName,
              customizedStateValue.name());
      updateLocalCustomizedViewMap(INSTANCE_0, customizedStateType, resourceName, partitionName,
          customizedStateValue);
    } else if (instanceName.equals(INSTANCE_1)) {
      _customizedStateProvider_participant1
          .updateCustomizedState(customizedStateType.name(), resourceName, partitionName,
              customizedStateValue.name());
      updateLocalCustomizedViewMap(INSTANCE_1, customizedStateType, resourceName, partitionName,
          customizedStateValue);
    } else {
      throw new Exception("The input instance name is not valid.");
    }
  }

  /**
   *
   * Call this method in the test for an delete on customized state in both ZK and local map
   * @param instanceName the instance to be updated
   * @param customizedStateType the customized state type to be updated
   * @param resourceName the resource to be updated
   * @param partitionName the partition to be updated
   * @throws Exception if the input instance name is not valid
   */
  private void delete(String instanceName, CustomizedStateType customizedStateType,
      String resourceName, String partitionName) throws Exception {
    if (instanceName.equals(INSTANCE_0)) {
      _customizedStateProvider_participant0
          .deletePerPartitionCustomizedState(customizedStateType.name(), resourceName,
              partitionName);
      updateLocalCustomizedViewMap(INSTANCE_0, customizedStateType, resourceName, partitionName,
          null);
    } else if (instanceName.equals(INSTANCE_1)) {
      _customizedStateProvider_participant1
          .deletePerPartitionCustomizedState(customizedStateType.name(), resourceName,
              partitionName);
      updateLocalCustomizedViewMap(INSTANCE_1, customizedStateType, resourceName, partitionName,
          null);
    } else {
      throw new Exception("The input instance name is not valid.");
    }
  }

  /**
   * Set the customized state types to be listened by routing table provider
   * @param customizedStateTypes a list of the types to listen
   */
  private void setTypesToListenInRoutingTableProvider(
      List<CustomizedStateType> customizedStateTypes) {
    List<String> enabledTypes = new ArrayList<>();
    _localVisibleCustomizedStateType.clear();
    for (CustomizedStateType type : customizedStateTypes) {
      enabledTypes.add(type.name());
      _localVisibleCustomizedStateType.add(type.name());
    }
    Map<PropertyType, List<String>> dataSource = new HashMap<>();
    dataSource.put(PropertyType.CUSTOMIZEDVIEW, enabledTypes);
    _routingTableProvider = new RoutingTableProvider(_spectator, dataSource);
  }

  /**
   * First update of customized state
   * Currently only aggregates CURRENT_STATE
   * instance    state type  resource    partition            key                 value
   * ---------------------------------------------------------------------------------
   *    0            A          A           1            CURRENT_STATE         StatusA1 - D
   *    0            B          A           1            CURRENT_STATE         StatusB1
   *    0            B          A           2            CURRENT_STATE         StatusB2  -M -> StatusB3
   *    0            A          B           2            CURRENT_STATE         StatusA2  -M -> StatusA1
   *    1            C          A           1            CURRENT_STATE         StatusC1
   *    1            C          A           2            CURRENT_STATE         StatusC2
   *    1            A          B           1            CURRENT_STATE         StatusA3 -D
   *    1            B          B           1            CURRENT_STATE         StatusB3 -D -M-> StatusB2
   *    1            C          B           1            CURRENT_STATE         StatusC3 -M -> StatusC1
   *
   *    -D: to be deleted in the test
   *    -M: to be modified in the test
   */
  @Test
  public void testCustomizedStateViewAggregation() throws Exception {

    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_A, PARTITION_A1,
        CurrentStateValues.StatusA1);
    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_A, PARTITION_A1,
        CurrentStateValues.StatusB1);
    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_A, PARTITION_A2,
        CurrentStateValues.StatusB2);
    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_B, PARTITION_B2,
        CurrentStateValues.StatusA2);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_A, PARTITION_A1,
        CurrentStateValues.StatusC1);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_A, PARTITION_A2,
        CurrentStateValues.StatusC2);
    update(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_B, PARTITION_B1,
        CurrentStateValues.StatusA3);
    update(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_B, PARTITION_B1,
        CurrentStateValues.StatusB3);

    // Test batch update API to update several customized states in the same customized state type for one resource, but for now only CURRENT_STATE will be aggregated in customized view
    Map<String, String> customizedStates = Maps.newHashMap();
    customizedStates.put("CURRENT_STATE", CurrentStateValues.StatusC3.name());
    customizedStates.put("PREVIOUS_STATE", CurrentStateValues.StatusC1.name());
    _customizedStateProvider_participant1
        .updateCustomizedState(CustomizedStateType.TYPE_C.name(), RESOURCE_B, PARTITION_B1,
            customizedStates);
    updateLocalCustomizedViewMap(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_B, PARTITION_B1,
        CurrentStateValues.StatusC3);

    // Only listen to Type A
    setTypesToListenInRoutingTableProvider(Arrays.asList(CustomizedStateType.TYPE_A));
    validateAggregationSnapshot();

    // Listens to all all three types
    setTypesToListenInRoutingTableProvider(Arrays
        .asList(CustomizedStateType.TYPE_A, CustomizedStateType.TYPE_B,
            CustomizedStateType.TYPE_C));
    validateAggregationSnapshot();

    // Update some customized states and verify
    delete(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_A, PARTITION_A1);
    validateAggregationSnapshot();

    delete(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_B, PARTITION_B1);
    validateAggregationSnapshot();

    delete(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_B, PARTITION_B1);
    validateAggregationSnapshot();

    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_A, PARTITION_A2,
        CurrentStateValues.StatusB3);
    validateAggregationSnapshot();

    update(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_B, PARTITION_B1,
        CurrentStateValues.StatusB2);
    validateAggregationSnapshot();

    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_B, PARTITION_B1,
        CurrentStateValues.StatusC1);
    validateAggregationSnapshot();

    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_B, PARTITION_B2,
        CurrentStateValues.StatusA1);
    validateAggregationSnapshot();
  }
}
