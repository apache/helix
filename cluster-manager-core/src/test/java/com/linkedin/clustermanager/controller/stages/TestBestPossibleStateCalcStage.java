package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;

@Test(groups =
{ "unitTest" })
public class TestBestPossibleStateCalcStage extends BaseStageTest
{

  @Test
  public void testSimple()
  {
  	System.out.println("START TestBestPossibleStateCalcStage at " + new Date(System.currentTimeMillis()));
    List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resourceGroups = new String[]
    { "testResourceGroupName" };
    setupIdealState(5, idealStates, resourceGroups);
    setupLiveInstances(5);
    setupStateModel();

    Map<String, ResourceGroup> resourceGroupMap = getResourceGroupMap();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(),
        resourceGroupMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(),
        currentStateOutput);

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output = event
        .getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++)
    {
      ResourceKey resource = new ResourceKey("testResourceGroupName_" + p);
      AssertJUnit.assertEquals(
          "MASTER",
          output.getInstanceStateMap("testResourceGroupName", resource).get(
              "localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at " + new Date(System.currentTimeMillis()));
  }
}
