package com.linkedin.helix.controller.stages;

import org.testng.annotations.Test;
import org.testng.AssertJUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.controller.stages.ClusterEvent;
@Test
public class TestClusterEvent
{
  @Test
  public void testSimplePutandGet(){
    ClusterEvent event = new ClusterEvent("name");
    AssertJUnit.assertEquals(event.getName(), "name");
    event.addAttribute("attr1", "value");
    AssertJUnit.assertEquals(event.getAttribute("attr1"), "value");
  }
}
