package com.linkedin.helix.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.model.IdealState.IdealStateModeProperty;

public class TestIdealState
{
  @Test
  public void testGetInstanceSet()
  {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;
    System.out.println("START " + testName + " at "
        + new Date(System.currentTimeMillis()));


    IdealState idealState = new IdealState("idealState");
    idealState.getRecord().setListField("TestDB_0", Arrays.asList("node_1", "node_2"));
    Map<String, String> instanceState = new HashMap<String, String>();
    instanceState.put("node_3", "MASTER");
    instanceState.put("node_4", "SLAVE");
    idealState.getRecord().setMapField("TestDB_1", instanceState);

    // test AUTO mode
    idealState.setIdealStateMode(IdealStateModeProperty.AUTO.toString());
    Set<String> instances = idealState.getInstanceSet("TestDB_0");
//    System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_1"), "Should contain node_1 and node_2");
    Assert.assertTrue(instances.contains("node_2"), "Should contain node_1 and node_2");

    instances = idealState.getInstanceSet("TestDB_nonExist_auto");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");
    
    // test CUSTOMIZED mode
    idealState.setIdealStateMode(IdealStateModeProperty.CUSTOMIZED.toString());
    instances = idealState.getInstanceSet("TestDB_1");
//    System.out.println("instances: " + instances);
    Assert.assertEquals(instances.size(), 2, "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_3"), "Should contain node_3 and node_4");
    Assert.assertTrue(instances.contains("node_4"), "Should contain node_3 and node_4");

    instances = idealState.getInstanceSet("TestDB_nonExist_custom");
    Assert.assertEquals(instances, Collections.emptySet(), "Should get empty set");
    
    System.out.println("END " + testName + " at "
        + new Date(System.currentTimeMillis()));
  }
}
