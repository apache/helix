package com.linkedin.clustermanager.controller.stages;

import org.testng.Assert;
import org.testng.annotations.Test;
@Test
public class TestClusterEvent
{
  public void testSimplePutandGet(){
    ClusterEvent event = new ClusterEvent("name");
    Assert.assertEquals(event.getName(), "name");
    event.addAttribute("attr1", "value");
    Assert.assertEquals(event.getAttribute("attr1"), "value");
  }
}
