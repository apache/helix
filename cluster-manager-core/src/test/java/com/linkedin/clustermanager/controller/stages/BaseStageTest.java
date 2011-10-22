package com.linkedin.clustermanager.controller.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.BeforeMethod;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterDataAccessor.IdealStateConfigProperty;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.model.ResourceGroup;
import com.linkedin.clustermanager.pipeline.Stage;
import com.linkedin.clustermanager.pipeline.StageContext;
import com.linkedin.clustermanager.tools.StateModelConfigGenerator;

public class BaseStageTest
{
  protected ClusterManager manager;
  protected ClusterDataAccessor accessor;
  protected ClusterEvent event;

  @BeforeMethod(groups = { "unitTest" })
  public void setup()
  {
    System.out.println("BaseStageTest.setup()");
    String clusterName = "testCluster-" + UUID.randomUUID().toString();
    manager = new Mocks.MockManager(clusterName);
    accessor = manager.getDataAccessor();
    event = new ClusterEvent("sampleEvent");

  }

  protected String[] setupIdealState(int nodes,
                                     List<IdealState> idealStates,
                                     String[] resourceGroups)
  {
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++)
    {
      instances.add("localhost_" + i);
    }

    for (int i = 0; i < resourceGroups.length; i++)
    {
      int partitions = 10;
      int replicas = 1;
      String resourceGroupName = resourceGroups[i];
      ZNRecord record = new ZNRecord(resourceGroupName);
      for (int p = 0; p < partitions; p++)
      {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++)
        {
          value.add("localhost_" + (p + r + 1) % nodes);
        }
        record.setListField(resourceGroupName + "_" + p, value);
      }
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefRef("MasterSlave");
      manager.getDataAccessor().setProperty(PropertyType.IDEALSTATES,
                                            idealState.getRecord(),
                                            resourceGroupName);
      IdealState idealstate = new IdealState(record);
      idealState.setIdealStateMode(IdealStateConfigProperty.AUTO.toString());
      idealStates.add(idealState);
    }
    return resourceGroups;
  }

  protected void setupLiveInstances(int numLiveInstances)
  {
    // setup liveInstances

    for (int i = 0; i < numLiveInstances; i++)
    {
      ZNRecord znRecord = new ZNRecord("localhost_" + i);
      LiveInstance liveInstance = new LiveInstance(znRecord);
      liveInstance.setSessionId("session_" + i);
      accessor.setProperty(PropertyType.LIVEINSTANCES, znRecord, "localhost_" + i);
    }
  }

  protected void runStage(ClusterEvent event, Stage stage)
  {
    event.addAttribute("clustermanager", manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    stage.postProcess();
  }

  protected void setupStateModel()
  {
    ZNRecord masterSlave = new StateModelConfigGenerator().generateConfigForMasterSlave();
    accessor.setProperty(PropertyType.STATEMODELDEFS, masterSlave, masterSlave.getId());
    ZNRecord leaderStandby = new StateModelConfigGenerator().generateConfigForLeaderStandby();
    accessor.setProperty(PropertyType.STATEMODELDEFS, leaderStandby, leaderStandby.getId());
    ZNRecord onlineOffline = new StateModelConfigGenerator().generateConfigForOnlineOffline();
    accessor.setProperty(PropertyType.STATEMODELDEFS, onlineOffline, onlineOffline.getId());
  }

  protected Map<String, ResourceGroup> getResourceGroupMap()
  {
    Map<String, ResourceGroup> resourceGroupMap = new HashMap<String, ResourceGroup>();
    ResourceGroup testResourceGroup = new ResourceGroup("testResourceGroupName");
    testResourceGroup.setStateModelDefRef("MasterSlave");
    testResourceGroup.addResource("testResourceGroupName_0");
    testResourceGroup.addResource("testResourceGroupName_1");
    testResourceGroup.addResource("testResourceGroupName_2");
    testResourceGroup.addResource("testResourceGroupName_3");
    testResourceGroup.addResource("testResourceGroupName_4");
    resourceGroupMap.put("testResourceGroupName", testResourceGroup);
    
    return resourceGroupMap;
  }
}
