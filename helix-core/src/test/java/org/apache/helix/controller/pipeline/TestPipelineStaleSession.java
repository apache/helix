package org.apache.helix.controller.pipeline;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Verifies that a pipeline stops processing an event whose session is not the controller's session
 * anymore, and keeps processing it in all other cases.
 */
public class TestPipelineStaleSession {
  private static final String CURRENT_SESSION = "session-2";
  private static final String EXPIRED_SESSION = "session-1";

  @Test
  public void testStagesRunWhenEventSessionIsCurrent() throws Exception {
    List<String> processed = new ArrayList<>();
    Pipeline pipeline = pipelineWithStages(processed);

    pipeline.handle(event(Optional.of(CURRENT_SESSION), connectedManager(CURRENT_SESSION)));

    Assert.assertEquals(processed, Arrays.asList("first", "second"));
  }

  @Test
  public void testPipelineStopsWhenEventSessionIsStale() throws Exception {
    List<String> processed = new ArrayList<>();
    Pipeline pipeline = pipelineWithStages(processed);

    try {
      pipeline.handle(event(Optional.of(EXPIRED_SESSION), connectedManager(CURRENT_SESSION)));
      Assert.fail("A stale event must not be processed");
    } catch (StageException expected) {
      Assert.assertTrue(expected.getMessage().contains(EXPIRED_SESSION), expected.getMessage());
    }
    Assert.assertTrue(processed.isEmpty(), "No stage should run for a stale event: " + processed);
  }

  @Test
  public void testStaleSessionIsDetectedBetweenStages() throws Exception {
    List<String> processed = new ArrayList<>();
    HelixManager manager = connectedManager(EXPIRED_SESSION);
    Pipeline pipeline = new Pipeline("DEFAULT");
    pipeline.addStage(new RecordingStage("first", processed));
    // The session changes while this stage is running.
    pipeline.addStage(new AbstractBaseStage() {
      @Override
      public void process(ClusterEvent event) {
        processed.add("session-change");
        Mockito.when(manager.getSessionId()).thenReturn(CURRENT_SESSION);
      }
    });
    pipeline.addStage(new RecordingStage("third", processed));

    try {
      pipeline.handle(event(Optional.of(EXPIRED_SESSION), manager));
      Assert.fail("The stage after the session change must not run");
    } catch (StageException expected) {
      // expected
    }
    Assert.assertEquals(processed, Arrays.asList("first", "session-change"));
  }

  @Test
  public void testStagesRunWhenEventHasNoSession() throws Exception {
    List<String> processed = new ArrayList<>();
    Pipeline pipeline = pipelineWithStages(processed);

    // Events built by tests carry no session attribute at all.
    ClusterEvent noSession = new ClusterEvent("cluster", ClusterEventType.OnDemandRebalance);
    noSession.addAttribute(AttributeName.helixmanager.name(), connectedManager(CURRENT_SESSION));
    pipeline.handle(noSession);
    // Events created while the controller was not the leader carry an empty session; those are
    // filtered by GenericHelixController before they reach a pipeline.
    pipeline.handle(event(Optional.empty(), connectedManager(CURRENT_SESSION)));
    // Events without a manager cannot be checked.
    ClusterEvent noManager = new ClusterEvent("cluster", ClusterEventType.OnDemandRebalance);
    noManager.addAttribute(AttributeName.EVENT_SESSION.name(), Optional.of(EXPIRED_SESSION));
    pipeline.handle(noManager);

    Assert.assertEquals(processed,
        Arrays.asList("first", "second", "first", "second", "first", "second"));
  }

  @Test
  public void testStagesRunWhileManagerIsDisconnected() throws Exception {
    List<String> processed = new ArrayList<>();
    Pipeline pipeline = pipelineWithStages(processed);

    // While disconnected the session is unknown, so the event is not stale. The manager may
    // either report not connected or throw from getSessionId().
    HelixManager disconnected = Mockito.mock(HelixManager.class);
    Mockito.when(disconnected.isConnected()).thenReturn(false);
    Mockito.when(disconnected.getSessionId()).thenReturn(CURRENT_SESSION);
    pipeline.handle(event(Optional.of(EXPIRED_SESSION), disconnected));

    HelixManager throwing = Mockito.mock(HelixManager.class);
    Mockito.when(throwing.isConnected()).thenReturn(true);
    Mockito.when(throwing.getSessionId()).thenThrow(new HelixException("not connected"));
    pipeline.handle(event(Optional.of(EXPIRED_SESSION), throwing));

    Assert.assertEquals(processed, Arrays.asList("first", "second", "first", "second"));
  }

  private static Pipeline pipelineWithStages(List<String> processed) {
    Pipeline pipeline = new Pipeline("DEFAULT");
    pipeline.addStage(new RecordingStage("first", processed));
    pipeline.addStage(new RecordingStage("second", processed));
    return pipeline;
  }

  private static ClusterEvent event(Optional<String> eventSession, HelixManager manager) {
    ClusterEvent event = new ClusterEvent("cluster", ClusterEventType.OnDemandRebalance);
    event.addAttribute(AttributeName.EVENT_SESSION.name(), eventSession);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    return event;
  }

  private static HelixManager connectedManager(String sessionId) {
    HelixManager manager = Mockito.mock(HelixManager.class);
    Mockito.when(manager.isConnected()).thenReturn(true);
    Mockito.when(manager.getSessionId()).thenReturn(sessionId);
    Mockito.when(manager.getInstanceName()).thenReturn("controller");
    return manager;
  }

  private static class RecordingStage extends AbstractBaseStage {
    private final String _name;
    private final List<String> _processed;

    RecordingStage(String name, List<String> processed) {
      _name = name;
      _processed = processed;
    }

    @Override
    public void process(ClusterEvent event) {
      _processed.add(_name);
    }

    @Override
    public String getStageName() {
      return _name;
    }
  }
}
