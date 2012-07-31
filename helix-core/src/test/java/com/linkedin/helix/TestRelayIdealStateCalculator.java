package com.linkedin.helix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.IdealStateCalculatorForEspressoRelay;

public class TestRelayIdealStateCalculator
{
  @Test ()
  public void testEspressoStorageClusterIdealState() throws Exception
  {
    testEspressoStorageClusterIdealState(15, 9, 3);
    testEspressoStorageClusterIdealState(15, 6, 3);
    testEspressoStorageClusterIdealState(15, 6, 2);
    testEspressoStorageClusterIdealState(6,  4, 2);
  }
  public void testEspressoStorageClusterIdealState(int partitions, int nodes, int replica) throws Exception
  {
    List<String> storageNodes = new ArrayList<String>();
    for(int i = 0;i < partitions; i++)
    {
      storageNodes.add("localhost:123" + i);
    }
    
    List<String> relays = new ArrayList<String>();
    for(int i = 0;i < nodes; i++)
    {
      relays.add("relay:123" + i);
    }
    
    IdealState idealstate = IdealStateCalculatorForEspressoRelay.calculateRelayIdealState(storageNodes, relays, "TEST", replica, "Leader", "Standby", "LeaderStandby");
    
    Assert.assertEquals(idealstate.getRecord().getListFields().size(), idealstate.getRecord().getMapFields().size());
    
    Map<String, Integer> countMap = new TreeMap<String, Integer>();
    for(String key  : idealstate.getRecord().getListFields().keySet())
    {
      Assert.assertEquals(idealstate.getRecord().getListFields().get(key).size(), idealstate.getRecord().getMapFields().get(key).size());
      List<String> list = idealstate.getRecord().getListFields().get(key);
      Map<String, String> map = idealstate.getRecord().getMapFields().get(key);
      Assert.assertEquals(list.size(), replica);
      for(String val : list)
      {
        if(!countMap.containsKey(val))
        {
          countMap.put(val, 1);
        }
        else
        {
          countMap.put(val, countMap.get(val) + 1);
        }
        Assert.assertTrue(map.containsKey(val));
      }
    }
    for(String nodeName : countMap.keySet())
    {
      Assert.assertTrue(countMap.get(nodeName) <= partitions * replica / nodes + 1);
      //System.out.println(nodeName + " " + countMap.get(nodeName));
    }
    System.out.println();
  }
}
