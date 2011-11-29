package com.linkedin.clustermanager.controller.stages;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.clustermanager.CMConstants;
import com.linkedin.clustermanager.Mocks;
import com.linkedin.clustermanager.PropertyType;
import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.IdealState;
import com.linkedin.clustermanager.model.LiveInstance;
import com.linkedin.clustermanager.pipeline.StageContext;
import com.linkedin.clustermanager.tools.IdealStateCalculatorForStorageNode;

public class TestCompatibilityCheckStage extends BaseStageTest
{
  @Test
  public void testCompatible()
  {
    List<String> instances = Arrays.asList("localhost_0", "localhost_1",
                                           "localhost_2", "localhost_3", "localhost_4");
    int partitions = 10;
    int replicas = 1;

    // set ideal state
    String resourceGroupName = "testResourceGroup";
    ZNRecord record = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, partitions, replicas, resourceGroupName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.IDEALSTATES,
        idealState.getRecord(), resourceGroupName);

    // set live instances
    record = new ZNRecord("localhost_0");
    record.setSimpleField(CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION.toString(), "0.4.0");
    LiveInstance liveInstance = new LiveInstance(record);
    liveInstance.setSessionId("session_0");

    accessor.setProperty(PropertyType.LIVEINSTANCES, record, "localhost_0");

    ((Mocks.MockManager)manager).setVersion("0.4.4");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    runStage(event, new ReadClusterDataStage());

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
      Assert.fail("Should not fail since versions are compatible");
    }
    stage.postProcess();
  }

  @Test
  public void testNullVersion()
  {
    List<String> instances = Arrays.asList("localhost_0", "localhost_1",
                                           "localhost_2", "localhost_3", "localhost_4");
    int partitions = 10;
    int replicas = 1;


    // set ideal state
    String resourceGroupName = "testResourceGroup";
    ZNRecord record = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, partitions, replicas, resourceGroupName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.IDEALSTATES,
        idealState.getRecord(), resourceGroupName);

    // set live instances
    record = new ZNRecord("localhost_0");
    LiveInstance liveInstance = new LiveInstance(record);
    liveInstance.setSessionId("session_0");
    accessor.setProperty(PropertyType.LIVEINSTANCES, record, "localhost_0");

    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    runStage(event, new ReadClusterDataStage());

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
      Assert.fail("Should not fail since versions are null");
    }
    stage.postProcess();
  }

  @Test
  public void testIncompatible()
  {
    List<String> instances = Arrays.asList("localhost_0", "localhost_1",
                                           "localhost_2", "localhost_3", "localhost_4");
    int partitions = 10;
    int replicas = 1;


    // set ideal state
    String resourceGroupName = "testResourceGroup";
    ZNRecord record = IdealStateCalculatorForStorageNode.calculateIdealState(
        instances, partitions, replicas, resourceGroupName, "MASTER", "SLAVE");
    IdealState idealState = new IdealState(record);
    idealState.setStateModelDefRef("MasterSlave");
    accessor.setProperty(PropertyType.IDEALSTATES,
        idealState.getRecord(), resourceGroupName);

    // set live instances
    record = new ZNRecord("localhost_0");
    record.setSimpleField(CMConstants.ZNAttribute.CLUSTER_MANAGER_VERSION.toString(), "0.3.4");
    LiveInstance liveInstance = new LiveInstance(record);
    liveInstance.setSessionId("session_0");

    accessor.setProperty(PropertyType.LIVEINSTANCES, record, "localhost_0");

    ((Mocks.MockManager)manager).setVersion("0.2.12");
    CompatibilityCheckStage stage = new CompatibilityCheckStage();
    runStage(event, new ReadClusterDataStage());

    event.addAttribute("clustermanager", manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try
    {
      stage.process(event);
      Assert.fail("Should fail since versions are incompatible");
    }
    catch (Exception e)
    {
      // OK
    }
    stage.postProcess();
  }

}
