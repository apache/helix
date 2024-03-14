package org.apache.helix.controller.rebalancer;

import java.util.List;
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Partition;
import org.apache.helix.util.TestInputLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMaintenanceRebalancer {

  private static final String RESOURCE_NAME = "testResource";
  private static final String PARTITION_NAME = "testResourcePartition";

  @Test(dataProvider = "TestComputeIdealStateInput")
  public void testComputeIdealState(String comment, String stateModelName, List<String> liveInstances,
      List<String> preferenceList, Map<String, String> currentStateMap, List<String> expectedPrefList) {
    System.out.println("Test case comment: " + comment);
    MaintenanceRebalancer rebalancer = new MaintenanceRebalancer();

    Partition partition = new Partition(PARTITION_NAME);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (String instance : currentStateMap.keySet()) {
      currentStateOutput.setCurrentState(RESOURCE_NAME, partition, instance, currentStateMap.get(instance));
    }

    IdealState currentIdealState = new IdealState(RESOURCE_NAME);
    currentIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    currentIdealState.setRebalancerClassName("org.apache.helix.controller.rebalancer.waged.WagedRebalancer");
    currentIdealState.setStateModelDefRef(stateModelName);
    currentIdealState.setPreferenceList(PARTITION_NAME, preferenceList);

    ResourceControllerDataProvider dataCache = mock(ResourceControllerDataProvider.class);
    when(dataCache.getStateModelDef("MasterSlave")).thenReturn(MasterSlaveSMD.build());

    IdealState updatedIdealState = rebalancer
        .computeNewIdealState(RESOURCE_NAME, currentIdealState, currentStateOutput, dataCache);

    List<String> partitionPrefList = updatedIdealState.getPreferenceList(PARTITION_NAME);
    Assert.assertTrue(partitionPrefList.equals(expectedPrefList));
  }

  @DataProvider(name = "TestComputeIdealStateInput")
  public Object[][] loadTestComputeIdealStateInput() {
    final String[] params = {
        "comment", "stateModel", "liveInstances", "preferenceList", "currentStateMap", "expectedPreferenceList"
    };
    return TestInputLoader.loadTestInputs("MaintenanceRebalancer.ComputeNewIdealState.json", params);
  }

}
