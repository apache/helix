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
import org.apache.helix.model.CustomizedState;
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
  // This map contains all the customized state information that is updated to ZooKeeper
  private static Map<String, Map<String, Map<String, Map<String, String>>>> _localCustomizedView;
  // The set contains customized state types that are enabled for aggregation in config
  private static Set<String> _aggregationEnabledTypes;
  // The set contains customized state types that routing table provider shows to users
  private static Set<String> _routingTableProviderDataSources;
  private String INSTANCE_0;
  private String INSTANCE_1;
  private final String RESOURCE_0 = "TestDB0";
  private final String RESOURCE_1 = "TestDB1";
  private final String PARTITION_00 = "TestDB0_0";
  private final String PARTITION_01 = "TestDB0_1";
  private final String PARTITION_10 = "TestDB1_0";
  private final String PARTITION_11 = "TestDB1_1";

  // Customized state values used for test, TYPE_A_0 - TYPE_A_2 are values for Customized state TypeA, etc.
  private enum CurrentStateValues {
    TYPE_A_0, TYPE_A_1, TYPE_A_2, TYPE_B_0, TYPE_B_1, TYPE_B_2, TYPE_C_0, TYPE_C_1, TYPE_C_2
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

    // Initialize customized state provider
    _customizedStateProvider_participant0 = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(_manager, participants[0].getInstanceName());
    _customizedStateProvider_participant1 = CustomizedStateProviderFactory.getInstance()
        .buildCustomizedStateProvider(_manager, participants[1].getInstanceName());

    _localCustomizedView = new HashMap<>();
    _routingTableProviderDataSources = new HashSet<>();
    _aggregationEnabledTypes = new HashSet<>();
  }

  @AfterClass
  public void afterClass() {
    _routingTableProvider.shutdown();
    _manager.disconnect();
    _spectator.disconnect();
  }

  /**
   * Compare the customized view values between ZK and local record
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
        boolean result = false;

        if (fullCustomizedViewSnapshot.isEmpty() && !_routingTableProviderDataSources.isEmpty()) {
          return false;
        }

        for (String customizedStateType : fullCustomizedViewSnapshot.keySet()) {
          if (!_routingTableProviderDataSources.contains(customizedStateType)) {
            return false;
          }

          // Get per customized state type snapshot
          RoutingTableSnapshot customizedViewSnapshot =
              fullCustomizedViewSnapshot.get(customizedStateType);

          // local per customized state type map
          Map<String, Map<String, Map<String, String>>> localSnapshot =
              _localCustomizedView.getOrDefault(customizedStateType, Maps.newHashMap());

          Collection<CustomizedView> customizedViews = customizedViewSnapshot.getCustomizeViews();

          // If a customized state is not set to be aggregated in config, but is enabled in routing table provider, it will show up in customized view returned to user, but will be empty
          if (!_aggregationEnabledTypes.contains(customizedStateType)
              && customizedViews.size() != 0) {
            return false;
          }

          // Get per resource snapshot
          for (CustomizedView resourceCustomizedView : customizedViews) {
            ZNRecord record = resourceCustomizedView.getRecord();
            Map<String, Map<String, String>> resourceStateMap = record.getMapFields();

            // Get local per resource map
            Map<String, Map<String, String>> localPerResourceCustomizedView = localSnapshot
                .getOrDefault(resourceCustomizedView.getResourceName(), Maps.newHashMap());

            if (resourceStateMap.isEmpty() && !localPerResourceCustomizedView.isEmpty()) {
              return false;
            }

            // Get per partition snapshot
            for (String partitionName : resourceStateMap.keySet()) {
              Map<String, String> stateMap =
                  resourceStateMap.getOrDefault(partitionName, Maps.newTreeMap());

              // Get local per partition map
              Map<String, String> localStateMap =
                  localPerResourceCustomizedView.getOrDefault(partitionName, Maps.newTreeMap());

              if (stateMap.isEmpty() && !localStateMap.isEmpty()) {
                return false;
              }

              for (String instanceName : stateMap.keySet()) {
                // Per instance value
                String stateMapValue = stateMap.get(instanceName);
                String localStateMapValue = localStateMap.get(instanceName);
                if (isEmptyValue(stateMapValue) && isEmptyValue(localStateMapValue)) {
                } else if ((!isEmptyValue(stateMapValue) && !isEmptyValue(localStateMapValue)
                    && !stateMapValue.equals(localStateMapValue)) || (isEmptyValue(stateMapValue)
                    || isEmptyValue(localStateMapValue))) {
                  return false;
                }
              }
            }
          }
        }
        return true;
      }
    }, 12000);

    Assert.assertTrue(result);
  }

  private boolean isEmptyValue(String value) {
    return value == null || value.equals("");
  }

  /**
   * Update the local record of customized view
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
      if (localPerPartition.isEmpty()) {
        localPerResource.remove(partitionName);
        if (localPerResource.isEmpty()) {
          localPerStateType.remove(resourceName);
          if (localPerStateType.isEmpty()) {
            _localCustomizedView.remove(customizedStateType.name());
          }
        }
      }
    } else {
      localPerPartition.put(instanceName, customizedStateValue.name());
    }
  }

  /**
   * Call this method in the test for an update on customized view in both ZK and local map
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
   * Call this method in the test for an delete on customized view in both ZK and local map
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
   * Set the data sources (customized state types) for routing table provider
   * @param customizedStateTypes list of customized state types that routing table provider will include in the snapshot shown to users
   */
  private void setRoutingTableProviderDataSources(List<CustomizedStateType> customizedStateTypes) {
    List<String> customizedViewSources = new ArrayList<>();
    _routingTableProviderDataSources.clear();
    for (CustomizedStateType type : customizedStateTypes) {
      customizedViewSources.add(type.name());
      _routingTableProviderDataSources.add(type.name());
    }
    Map<PropertyType, List<String>> dataSource = new HashMap<>();
    dataSource.put(PropertyType.CUSTOMIZEDVIEW, customizedViewSources);
    _routingTableProvider = new RoutingTableProvider(_spectator, dataSource);
  }

  /**
   * Set the customized view aggregation config in controller
   * @param aggregationEnabledTypes list of customized state types that the controller will aggregate to customized view
   */
  private void setAggregationEnabledTypes(List<CustomizedStateType> aggregationEnabledTypes) {
    List<String> enabledTypes = new ArrayList<>();
    _aggregationEnabledTypes.clear();
    for (CustomizedStateType type : aggregationEnabledTypes) {
      enabledTypes.add(type.name());
      _aggregationEnabledTypes.add(type.name());
    }
    CustomizedStateConfig.Builder customizedStateConfigBuilder =
        new CustomizedStateConfig.Builder();
    customizedStateConfigBuilder.setAggregationEnabledTypes(enabledTypes);
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    accessor.setProperty(accessor.keyBuilder().customizedStateConfig(),
        customizedStateConfigBuilder.build());
  }

  @Test
  public void testCustomizedStateViewAggregation() throws Exception {
    setAggregationEnabledTypes(
        Arrays.asList(CustomizedStateType.TYPE_A, CustomizedStateType.TYPE_B));

    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_0, PARTITION_00,
        CurrentStateValues.TYPE_A_0);
    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_0, PARTITION_00,
        CurrentStateValues.TYPE_B_0);
    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_0, PARTITION_01,
        CurrentStateValues.TYPE_B_1);
    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_11,
        CurrentStateValues.TYPE_A_1);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_0, PARTITION_00,
        CurrentStateValues.TYPE_C_0);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_0, PARTITION_01,
        CurrentStateValues.TYPE_C_1);
    update(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_1, PARTITION_10,
        CurrentStateValues.TYPE_B_2);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_1, PARTITION_10,
        CurrentStateValues.TYPE_C_2);
    update(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_11,
        CurrentStateValues.TYPE_A_1);

    // Aggregation enabled types: A, B; Routing table provider data sources: A, B, C; should show TypeA, TypeB customized views
    setRoutingTableProviderDataSources(Arrays
        .asList(CustomizedStateType.TYPE_A, CustomizedStateType.TYPE_B,
            CustomizedStateType.TYPE_C));
    validateAggregationSnapshot();

    // Aggregation enabled types: A, B; Routing table provider data sources: A, B, C; should show TypeA, TypeB customized views
    setAggregationEnabledTypes(Arrays.asList(CustomizedStateType.TYPE_A, CustomizedStateType.TYPE_B,
        CustomizedStateType.TYPE_C));
    validateAggregationSnapshot();

    Assert.assertNull(_customizedStateProvider_participant0
        .getCustomizedState(CustomizedStateType.TYPE_C.name(), RESOURCE_0));

    // Test batch update API to update several customized state fields in the same customized state, but for now only CURRENT_STATE will be aggregated in customized view
    Map<String, String> customizedStates = Maps.newHashMap();
    customizedStates.put("CURRENT_STATE", CurrentStateValues.TYPE_A_2.name());
    customizedStates.put("PREVIOUS_STATE", CurrentStateValues.TYPE_A_0.name());
    _customizedStateProvider_participant1
        .updateCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1, PARTITION_10,
            customizedStates);
    updateLocalCustomizedViewMap(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_10,
        CurrentStateValues.TYPE_A_2);

    // Aggregation enabled types: A, B, C; Routing table provider data sources: A; should only show TypeA customized view
    setRoutingTableProviderDataSources(Arrays.asList(CustomizedStateType.TYPE_A));
    validateAggregationSnapshot();

    // Test get customized state and get per partition customized state via customized state provider, this part of test doesn't change customized view
    CustomizedState customizedState = _customizedStateProvider_participant1
        .getCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1);
    Assert.assertEquals(customizedState.getState(PARTITION_10), CurrentStateValues.TYPE_A_2.name());
    Assert.assertEquals(customizedState.getPreviousState(PARTITION_10),
        CurrentStateValues.TYPE_A_0.name());
    Assert.assertEquals(customizedState.getState(PARTITION_11), CurrentStateValues.TYPE_A_1.name());
    Map<String, String> perPartitionCustomizedState = _customizedStateProvider_participant1
        .getPerPartitionCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1,
            PARTITION_10);
    Map<String, String> actualPerPartitionCustomizedState = Maps.newHashMap();
    actualPerPartitionCustomizedState
        .put(CustomizedState.CustomizedStateProperty.CURRENT_STATE.name(),
            CurrentStateValues.TYPE_A_2.name());
    actualPerPartitionCustomizedState
        .put(CustomizedState.CustomizedStateProperty.PREVIOUS_STATE.name(),
            CurrentStateValues.TYPE_A_0.name());
    Assert.assertEquals(perPartitionCustomizedState, actualPerPartitionCustomizedState);

    // Test delete per partition customized state via customized state provider.
    _customizedStateProvider_participant1
        .deletePerPartitionCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1,
            PARTITION_10);
    customizedState = _customizedStateProvider_participant1
        .getCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1);
    Assert.assertEquals(customizedState.getState(PARTITION_11), CurrentStateValues.TYPE_A_1.name());
    Assert.assertNull(_customizedStateProvider_participant1
        .getPerPartitionCustomizedState(CustomizedStateType.TYPE_A.name(), RESOURCE_1,
            PARTITION_10));
    // Customized view only reflect CURRENT_STATE field
    updateLocalCustomizedViewMap(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_10,
        null);
    validateAggregationSnapshot();

    //Aggregation enabled types: B; Routing table provider data sources: A, B, C; should show TypeB customized views
    setAggregationEnabledTypes(Arrays.asList(CustomizedStateType.TYPE_B));
    setRoutingTableProviderDataSources(Arrays
        .asList(CustomizedStateType.TYPE_A, CustomizedStateType.TYPE_B,
            CustomizedStateType.TYPE_C));
    validateAggregationSnapshot();

    //Aggregation enabled types: B; Routing table provider data sources: A; should show empty customized view
    setRoutingTableProviderDataSources(Arrays.asList(CustomizedStateType.TYPE_A));
    validateAggregationSnapshot();

    // Update some customized states and verify
    delete(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_0, PARTITION_00);
    delete(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_1, PARTITION_10);

    // delete a customize state that does not exist
    delete(INSTANCE_1, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_10);
    validateAggregationSnapshot();

    update(INSTANCE_0, CustomizedStateType.TYPE_B, RESOURCE_0, PARTITION_01,
        CurrentStateValues.TYPE_B_2);
    update(INSTANCE_1, CustomizedStateType.TYPE_B, RESOURCE_1, PARTITION_10,
        CurrentStateValues.TYPE_B_1);
    update(INSTANCE_1, CustomizedStateType.TYPE_C, RESOURCE_1, PARTITION_10,
        CurrentStateValues.TYPE_C_0);
    update(INSTANCE_0, CustomizedStateType.TYPE_A, RESOURCE_1, PARTITION_11,
        CurrentStateValues.TYPE_A_0);
    validateAggregationSnapshot();
  }
}
