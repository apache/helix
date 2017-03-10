package org.apache.helix.controller.rebalancer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.TestHelper;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestAbstractRebalancer {

  @Test(dataProvider = "TestComputeBestPossibleStateInput")
  public void testComputeBestPossibleState(String stateModelName, List<String> liveInstances,
      List<String> preferenceList, Map<String, String> currentStateMap, List<String> disabledInstancesForPartition,
      Map<String, String> expectedBestPossibleMap, String comment) {
    System.out.println("START " + TestHelper.getTestMethodName() + " at " + new Date(System.currentTimeMillis()));
    System.out.println("Test case comment: " + comment);
    AutoRebalancer rebalancer = new AutoRebalancer();
    Map<String, String> bestPossibleMap = rebalancer
        .computeBestPossibleStateForPartition(new HashSet<String>(liveInstances),
            BuiltInStateModelDefinitions.valueOf(stateModelName).getStateModelDefinition(),
            preferenceList, currentStateMap, new HashSet<String>(disabledInstancesForPartition), true);

    Assert.assertEquals(bestPossibleMap.size(), expectedBestPossibleMap.size());
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap);
    System.out.println("END " + TestHelper.getTestMethodName() + " at " + new Date(System.currentTimeMillis()));
  }

  private final String[] params = {"stateModel", "liveInstances", "preferenceList", "currentStateMap",
      "disabledInstancesForPartition", "expectedBestPossibleStateMap", "comment"};

  @DataProvider(name = "TestComputeBestPossibleStateInput")
  public Object[][] loadTestComputeBestPossibleStateInput() {
    return loadTestInputs("TestAbstractRebalancer.ComputeBestPossibleState.json");
  }

  private Object[][] loadTestInputs(String inputFile) {
    List<Object[]> data = new ArrayList<Object[]>();
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(inputFile);
    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map[].class);
      Map[] inputs = mapReader.readValue(inputStream);

      for (Map input : inputs) {
        Object[] objects = new Object[params.length];
        for (int i = 0; i < params.length; i++) {
          objects[i] = input.get(params[i]);
        }
        data.add(objects);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    Object[][] ret = new Object[data.size()][];
    for(int i = 0; i < data.size(); i++) {
      ret[i] = data.get(i);
    }
    return ret;
  }
}
