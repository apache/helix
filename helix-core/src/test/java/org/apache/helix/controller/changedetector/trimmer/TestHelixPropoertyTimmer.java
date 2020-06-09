package org.apache.helix.controller.changedetector.trimmer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.changedetector.trimmer.HelixPropertyTrimmer.FieldType;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;

public class TestHelixPropoertyTimmer {
  private final String CLUSTER_NAME = "CLUSTER";
  private final String INSTANCE_NAME = "INSTANCE";
  private final String RESOURCE_NAME = "RESOURCE";

  private final Set<HelixConstants.ChangeType> _changeTypes = new HashSet<>();
  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  private final Map<String, IdealState> _idealStateMap = new HashMap<>();
  private final Map<String, ResourceConfig> _resourceConfigMap = new HashMap<>();
  private ClusterConfig _clusterConfig;
  private ResourceControllerDataProvider _dataProvider;

  @BeforeMethod
  public void beforeMethod() {
    _changeTypes.clear();
    _instanceConfigMap.clear();
    _idealStateMap.clear();
    _resourceConfigMap.clear();

    _changeTypes.add(HelixConstants.ChangeType.INSTANCE_CONFIG);
    _changeTypes.add(HelixConstants.ChangeType.IDEAL_STATE);
    _changeTypes.add(HelixConstants.ChangeType.RESOURCE_CONFIG);
    _changeTypes.add(HelixConstants.ChangeType.CLUSTER_CONFIG);

    _instanceConfigMap.put(INSTANCE_NAME, new InstanceConfig(INSTANCE_NAME));
    IdealState idealState = new IdealState(RESOURCE_NAME);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    _idealStateMap.put(RESOURCE_NAME, idealState);
    _resourceConfigMap.put(RESOURCE_NAME, new ResourceConfig(RESOURCE_NAME));
    _clusterConfig = new ClusterConfig(CLUSTER_NAME);

    _dataProvider =
        getMockDataProvider(_changeTypes, _instanceConfigMap, _idealStateMap, _resourceConfigMap,
            _clusterConfig);
  }

  private ResourceControllerDataProvider getMockDataProvider(
      Set<HelixConstants.ChangeType> changeTypes, Map<String, InstanceConfig> instanceConfigMap,
      Map<String, IdealState> idealStateMap, Map<String, ResourceConfig> resourceConfigMap,
      ClusterConfig clusterConfig) {
    ResourceControllerDataProvider dataProvider =
        Mockito.mock(ResourceControllerDataProvider.class);
    when(dataProvider.getRefreshedChangeTypes()).thenReturn(changeTypes);
    when(dataProvider.getInstanceConfigMap()).thenReturn(instanceConfigMap);
    when(dataProvider.getIdealStates()).thenReturn(idealStateMap);
    when(dataProvider.getResourceConfigMap()).thenReturn(resourceConfigMap);
    when(dataProvider.getClusterConfig()).thenReturn(clusterConfig);
    when(dataProvider.getLiveInstances()).thenReturn(Collections.emptyMap());
    return dataProvider;
  }

  @Test
  public void testDetectNonTrimmableFieldChanges() {
    // Fill mock data to initialize the detector
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    // Verify that all the non-trimmable field changes will be detected
    // 1. Cluster Config
    changeNonTrimmableValuesAndVerifyDetector(
        ClusterConfigTrimmer.getInstance().getNonTrimmableFields(_clusterConfig), _clusterConfig,
        HelixConstants.ChangeType.CLUSTER_CONFIG, detector, _dataProvider);
    // 2. Ideal States
    for (IdealState idealState : _idealStateMap.values()) {
      changeNonTrimmableValuesAndVerifyDetector(
          IdealStateTrimmer.getInstance().getNonTrimmableFields(idealState), idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);

      // Additional test to ensure Ideal State map/list fields are detected correctly according to
      // the rebalance mode.
      // For the following test, we can only focus on the smaller scope defined by the following map.
      Map<FieldType, Set<String>> overwriteFieldMap = new HashMap<>();

      // SEMI_AUTO: List fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      overwriteFieldMap.put(FieldType.LIST_FIELD, Collections.singleton("partitionList_SEMI_AUTO"));
      changeNonTrimmableValuesAndVerifyDetector(overwriteFieldMap, idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);

      // CUSTOMZIED: List and Map fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      overwriteFieldMap.clear();
      overwriteFieldMap
          .put(FieldType.LIST_FIELD, Collections.singleton("partitionList_CUSTOMIZED"));
      overwriteFieldMap.put(FieldType.MAP_FIELD, Collections.singleton("partitionMap_CUSTOMIZED"));
      changeNonTrimmableValuesAndVerifyDetector(overwriteFieldMap, idealState,
          HelixConstants.ChangeType.IDEAL_STATE, detector, _dataProvider);
    }
    // 3. Resource Config
    for (ResourceConfig resourceConfig : _resourceConfigMap.values()) {
      // Add a non-trimmable preference list to the resource config, this change should be detected as well.
      Map<String, List<String>> preferenceList = new HashMap<>();
      preferenceList.put("partitionList_ResourceConfig", Collections.emptyList());
      resourceConfig.setPreferenceLists(preferenceList);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      changeNonTrimmableValuesAndVerifyDetector(
          ResourceConfigTrimmer.getInstance().getNonTrimmableFields(resourceConfig), resourceConfig,
          HelixConstants.ChangeType.RESOURCE_CONFIG, detector, _dataProvider);
    }
    // 4. Instance Config
    for (InstanceConfig instanceConfig : _instanceConfigMap.values()) {
      changeNonTrimmableValuesAndVerifyDetector(
          InstanceConfigTrimmer.getInstance().getNonTrimmableFields(instanceConfig), instanceConfig,
          HelixConstants.ChangeType.INSTANCE_CONFIG, detector, _dataProvider);
    }
  }

  @Test
  public void testIgnoreTrimmableFieldChanges() {
    // Fill mock data to initialize the detector
    ResourceChangeDetector detector = new ResourceChangeDetector(true);
    detector.updateSnapshots(_dataProvider);

    // Verify that all the trimmable field changes will not be detected
    // 1. Cluster Config
    changeTrimmableValuesAndVerifyDetector(FieldType.values(), _clusterConfig, detector,
        _dataProvider);
    // 2. Ideal States
    for (IdealState idealState : _idealStateMap.values()) {
      changeTrimmableValuesAndVerifyDetector(FieldType.values(), idealState, detector,
          _dataProvider);
      // Additional test to ensure Ideal State map/list fields are detected correctly according to
      // the rebalance mode.

      // SEMI_AUTO: List fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      changeTrimmableValuesAndVerifyDetector(
          new FieldType[] { FieldType.SIMPLE_FIELD, FieldType.MAP_FIELD }, idealState, detector,
          _dataProvider);

      // CUSTOMZIED: List and Map fields are non-trimmable
      idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
      // refresh the detector cache after modification to avoid unexpected change detected.
      detector.updateSnapshots(_dataProvider);
      changeTrimmableValuesAndVerifyDetector(new FieldType[] { FieldType.SIMPLE_FIELD }, idealState,
          detector, _dataProvider);
    }
    // 3. Resource Config
    for (ResourceConfig resourceConfig : _resourceConfigMap.values()) {
      // Preference lists in the list fields are non-trimmable
      changeTrimmableValuesAndVerifyDetector(
          new FieldType[] { FieldType.SIMPLE_FIELD, FieldType.MAP_FIELD }, resourceConfig, detector,
          _dataProvider);
    }
    // 4. Instance Config
    for (InstanceConfig instanceConfig : _instanceConfigMap.values()) {
      changeTrimmableValuesAndVerifyDetector(FieldType.values(), instanceConfig, detector,
          _dataProvider);
    }
  }

  private void changeNonTrimmableValuesAndVerifyDetector(
      Map<FieldType, Set<String>> nonTrimmableFieldMap, HelixProperty helixProperty,
      HelixConstants.ChangeType expectedChangeType, ResourceChangeDetector detector,
      ResourceControllerDataProvider dataProvider) {
    for (FieldType type : nonTrimmableFieldMap.keySet()) {
      for (String fieldKey : nonTrimmableFieldMap.get(type)) {
        switch (type) {
        case LIST_FIELD:
          helixProperty.getRecord().setListField(fieldKey, Collections.singletonList("foobar"));
          break;
        case MAP_FIELD:
          helixProperty.getRecord().setMapField(fieldKey, Collections.singletonMap("foo", "bar"));
          break;
        case SIMPLE_FIELD:
          helixProperty.getRecord().setSimpleField(fieldKey, "foobar");
          break;
        default:
          Assert.fail("Unknown field type " + type.name());
        }
        detector.updateSnapshots(dataProvider);
        for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
          Assert.assertEquals(detector.getAdditionsByType(changeType).size(), 0,
              String.format("There should not be any additional change detected!"));
          Assert.assertEquals(detector.getRemovalsByType(changeType).size(), 0,
              String.format("There should not be any removal change detected!"));
          Assert.assertEquals(detector.getChangesByType(changeType).size(),
              changeType == expectedChangeType ? 1 : 0,
              String.format("The detected change of %s is not as expected.", fieldKey));
        }
      }
    }
  }

  private void changeTrimmableValuesAndVerifyDetector(FieldType[] trimmableFieldTypes,
      HelixProperty helixProperty, ResourceChangeDetector detector,
      ResourceControllerDataProvider dataProvider) {
    for (FieldType type : trimmableFieldTypes) {
      switch (type) {
      case LIST_FIELD:
        helixProperty.getRecord()
            .setListField("TrimmableListField", Collections.singletonList("foobar"));
        break;
      case MAP_FIELD:
        helixProperty.getRecord()
            .setMapField("TrimmableMapField", Collections.singletonMap("foo", "bar"));
        break;
      case SIMPLE_FIELD:
        helixProperty.getRecord().setSimpleField("TrimmableSimpleField", "foobar");
        break;
      default:
        Assert.fail("Unknown field type " + type.name());
      }
      detector.updateSnapshots(dataProvider);
      for (HelixConstants.ChangeType changeType : HelixConstants.ChangeType.values()) {
        Assert.assertEquals(
            detector.getAdditionsByType(changeType).size() + detector.getRemovalsByType(changeType)
                .size() + detector.getChangesByType(changeType).size(), 0, String.format(
                "There should not be any change detected for the trimmable field changes!"));
      }
    }
  }
}
