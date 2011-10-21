package com.linkedin.clustermanager.controller.stages;

import java.util.UUID;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;

import com.linkedin.clustermanager.ClusterDataAccessor;
import com.linkedin.clustermanager.ClusterManager;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.pipeline.Stage;
import com.linkedin.clustermanager.pipeline.StageContext;

public class BaseStageTest
{
  protected ClusterManager manager;
  protected ClusterDataAccessor accessor;
  protected ClusterEvent event;

  @BeforeMethod(groups={"unitTest"})
  public void setup()
  {
    System.out.println("BaseStageTest.setup()");
    String clusterName = "testCluster-" +UUID.randomUUID().toString();
    manager = new Mocks.MockManager(clusterName);
    accessor = manager.getDataAccessor();
    event = new ClusterEvent("sampleEvent");

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
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    stage.postProcess();
  }
}
