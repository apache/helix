package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.model.ResourceKey;

@Test(groups = { "unitTest" })
public class TestBestPossibleStateCalcStage extends BaseStageTest
{

  public void testSimple()
  {

    List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resourceGroups = new String[] { "testResourceGroup" };
    setupIdealState(5, idealStates, resourceGroups);
    setupLiveInstances(5);
    setupStateModel();
    Map<String, ResourceGroup> resourceGroupMap = getResourceGroupMap();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    // for(int p=0;p<5;p++){
    // output.setCurrentState("testResourceGroup", new ResourceKey("testResourceGroup_"+
    // p), "localhost_"+ (p+1)%5, "OFFLINE");
    // output.setPendingState("testResourceGroup", new ResourceKey("testResourceGroup_"+
    // p), "localhost_"+ (p+1)%5, "SLAVE");
    // }
    event.addAttribute(AttributeName.RESOURCE_GROUPS.toString(), resourceGroupMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
    
  

    BestPossibleStateCalcStage stage = new BestPossibleStateCalcStage();
    runStage(event, stage);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++)
    {
      ResourceKey resource = new ResourceKey("testResourceGroupName_"+ p);
      Assert.assertEquals("MASTER", output.getInstanceStateMap("testResourceGroupName", resource).get("localhost_"+ (p+1)%5));
    }

  }
}
