package com.linkedin.helix.controller.stages;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.controller.stages.MessageSelectionStage.Bounds;
import com.linkedin.helix.model.LiveInstance;
import com.linkedin.helix.model.Message;

public class TestMsgSelectionStage
{
  @Test
  public void testMasterXfer()
  {
    System.out.println("START testMasterXfer at " + new Date(System.currentTimeMillis()));

    Map<String, LiveInstance> liveInstances = new HashMap<String, LiveInstance>();
    liveInstances.put("localhost_0", new LiveInstance("localhost_0"));
    liveInstances.put("localhost_1", new LiveInstance("localhost_1"));

    Map<String, String> currentStates = new HashMap<String, String>();
    currentStates.put("localhost_0", "SLAVE");
    currentStates.put("localhost_1", "MASTER");

    Map<String, String> pendingStates = new HashMap<String, String>();

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage("msgId_0",
                                          "SLAVE",
                                          "MASTER",
                                          "localhost_0",
                                          "TestDB",
                                          "TestDB_0"));
    messages.add(TestHelper.createMessage("msgId_1",
                                          "MASTER",
                                          "SLAVE",
                                          "localhost_1",
                                          "TestDB",
                                          "TestDB_0"));

    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();
    stateConstraints.put("MASTER", new Bounds(0, 1));
    stateConstraints.put("SLAVE", new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);


    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveInstances,
                                                   currentStates,
                                                   pendingStates,
                                                   messages,
                                                   stateConstraints,
                                                   stateTransitionPriorities,
                                                   "OFFLINE");

    Assert.assertEquals(selectedMsg.size(), 1);
    Assert.assertEquals(selectedMsg.get(0).getMsgId(), "msgId_1");
    System.out.println("END testMasterXfer at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testMasterXferAfterMasterResume()
  {
    System.out.println("START testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));

    Map<String, LiveInstance> liveInstances = new HashMap<String, LiveInstance>();
    liveInstances.put("localhost_0", new LiveInstance("localhost_0"));
    liveInstances.put("localhost_1", new LiveInstance("localhost_1"));

    Map<String, String> currentStates = new HashMap<String, String>();
    currentStates.put("localhost_0", "SLAVE");
    currentStates.put("localhost_1", "SLAVE");

    Map<String, String> pendingStates = new HashMap<String, String>();
    pendingStates.put("localhost_1", "MASTER");

    List<Message> messages = new ArrayList<Message>();
    messages.add(TestHelper.createMessage("msgId_0",
                                          "SLAVE",
                                          "MASTER",
                                          "localhost_0",
                                          "TestDB",
                                          "TestDB_0"));

    Map<String, Bounds> stateConstraints = new HashMap<String, Bounds>();
    stateConstraints.put("MASTER", new Bounds(0, 1));
    stateConstraints.put("SLAVE", new Bounds(0, 2));

    Map<String, Integer> stateTransitionPriorities = new HashMap<String, Integer>();
    stateTransitionPriorities.put("MASTER-SLAVE", 0);
    stateTransitionPriorities.put("SLAVE-MASTER", 1);

    List<Message> selectedMsg =
        new MessageSelectionStage().selectMessages(liveInstances,
                                                   currentStates,
                                                   pendingStates,
                                                   messages,
                                                   stateConstraints,
                                                   stateTransitionPriorities,
                                                   "OFFLINE");

    Assert.assertEquals(selectedMsg.size(), 0);
    System.out.println("END testMasterXferAfterMasterResume at "
        + new Date(System.currentTimeMillis()));
  }
}
