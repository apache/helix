package com.linkedin.helix.controller.stages;

import java.util.Date;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.controller.stages.AttributeName;
import com.linkedin.helix.controller.stages.BestPossibleStateCalcStage;
import com.linkedin.helix.controller.stages.BestPossibleStateOutput;
import com.linkedin.helix.controller.stages.CurrentStateOutput;
import com.linkedin.helix.controller.stages.ReadClusterDataStage;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.Partition;

public class TestBestPossibleStateCalcStage extends BaseStageTest
{
  @Test
  public void testSimple()
  {
  	System.out.println("START TestBestPossibleStateCalcStage at " + new Date(System.currentTimeMillis()));
//    List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resources = new String[]{ "testResourceName" };
    setupIdealState(5, resources, 10, 1);
    setupLiveInstances(5);
    setupStateModel();

    Map<String, Resource> resourceMap = getResourceMap();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.toString(),
        resourceMap);
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
      Partition resource = new Partition("testResourceName_" + p);
      AssertJUnit.assertEquals(
          "MASTER",
          output.getInstanceStateMap("testResourceName", resource).get(
              "localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at " + new Date(System.currentTimeMillis()));
  }
}
