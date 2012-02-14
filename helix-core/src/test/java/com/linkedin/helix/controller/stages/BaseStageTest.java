package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import com.linkedin.helix.DataAccessor;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.Mocks;
import com.linkedin.helix.PropertyType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.controller.pipeline.Stage;
import com.linkedin.helix.controller.pipeline.StageContext;
import com.linkedin.helix.controller.stages.ClusterEvent;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Resource;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;
import com.linkedin.helix.tools.StateModelConfigGenerator;

public class BaseStageTest
{
  protected HelixManager manager;
  protected DataAccessor accessor;
  protected ClusterEvent event;

  @BeforeClass()
  public void beforeClass()
  {
    String className = this.getClass().getName();
    System.out.println("START " + className.substring(className.lastIndexOf('.') + 1)
                       + " at "+ new Date(System.currentTimeMillis()));
  }

  @AfterClass()
  public void afterClass()
  {
    String className = this.getClass().getName();
    System.out.println("END " + className.substring(className.lastIndexOf('.') + 1)
                       + " at "+ new Date(System.currentTimeMillis()));
  }

  @BeforeMethod()
  public void setup()
  {
    String clusterName = "testCluster-" + UUID.randomUUID().toString();
    manager = new Mocks.MockManager(clusterName);
    accessor = manager.getDataAccessor();
    event = new ClusterEvent("sampleEvent");
  }

  protected List<IdealState> setupIdealState(int nodes, String[] resources,
                                             int partitions, int replicas)
  {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++)
    {
      instances.add("localhost_" + i);
    }

    for (int i = 0; i < resources.length; i++)
    {
      String resourceName = resources[i];
      ZNRecord record = new ZNRecord(resourceName);
      for (int p = 0; p < partitions; p++)
      {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++)
        {
          value.add("localhost_" + (p + r + 1) % nodes);
        }
        record.setListField(resourceName + "_" + p, value);
      }
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

//      System.out.println(idealState);
      accessor.setProperty(PropertyType.IDEALSTATES,
                           idealState,
                           resourceName);
    }
    return idealStates;
  }

  protected void setupLiveInstances(int numLiveInstances)
  {
    // setup liveInstances
    for (int i = 0; i < numLiveInstances; i++)
    {
      LiveInstance liveInstance = new LiveInstance("localhost_" + i);
      liveInstance.setSessionId("session_" + i);
      accessor.setProperty(PropertyType.LIVEINSTANCES, liveInstance, "localhost_" + i);
    }
  }

  protected void runStage(ClusterEvent event, Stage stage)
  {
    event.addAttribute("helixmanager", manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    stage.postProcess();
  }

  protected void setupStateModel()
  {
    ZNRecord masterSlave = new StateModelConfigGenerator()
        .generateConfigForMasterSlave();
    accessor.setProperty(PropertyType.STATEMODELDEFS, masterSlave, masterSlave.getId());
    ZNRecord leaderStandby = new StateModelConfigGenerator()
        .generateConfigForLeaderStandby();
    accessor.setProperty(PropertyType.STATEMODELDEFS, leaderStandby, leaderStandby.getId());
    ZNRecord onlineOffline = new StateModelConfigGenerator()
        .generateConfigForOnlineOffline();
    accessor.setProperty(PropertyType.STATEMODELDEFS, onlineOffline, onlineOffline.getId());
  }

  protected Map<String, Resource> getResourceMap()
  {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();
    Resource testResource = new Resource("testResourceName");
    testResource.setStateModelDefRef("MasterSlave");
    testResource.addPartition("testResourceName_0");
    testResource.addPartition("testResourceName_1");
    testResource.addPartition("testResourceName_2");
    testResource.addPartition("testResourceName_3");
    testResource.addPartition("testResourceName_4");
    resourceMap.put("testResourceName", testResource);

    return resourceMap;
  }
}
