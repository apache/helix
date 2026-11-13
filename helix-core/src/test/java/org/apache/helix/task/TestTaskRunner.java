package org.apache.helix.task;

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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestTaskRunner {
  private static final String SESSION_ID = "session";

  @Test(timeOut = 5000)
  public void testRetriesTerminalStateUpdateAfterTemporaryDisconnect() {
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mockAccessor();
    when(manager.getHelixDataAccessor())
        .thenThrow(new HelixException("not connected"))
        .thenReturn(accessor);
    when(manager.getSessionId()).thenReturn(SESSION_ID);
    TaskStateModel stateModel = mock(TaskStateModel.class);

    TaskRunner runner = new TaskRunner(completedTask(), "job", "job_0", "instance", manager,
        SESSION_ID, stateModel);
    runner.run();

    Assert.assertEquals(runner.waitTillDone().getStatus(), TaskResult.Status.COMPLETED);
    verify(manager, times(2)).getHelixDataAccessor();
    verify(accessor).updateProperty(any(PropertyKey.class), any(CurrentState.class));
    verify(stateModel).updateState(TaskPartitionState.COMPLETED.name());
  }

  @Test(timeOut = 5000)
  public void testDoesNotWriteTaskStateAfterSessionChanges() {
    HelixManager manager = mock(HelixManager.class);
    HelixDataAccessor accessor = mockAccessor();
    when(manager.getHelixDataAccessor())
        .thenThrow(new HelixException("not connected"))
        .thenReturn(accessor);
    when(manager.getSessionId()).thenReturn("new-session");
    TaskStateModel stateModel = mock(TaskStateModel.class);

    TaskRunner runner = new TaskRunner(completedTask(), "job", "job_0", "instance", manager,
        SESSION_ID, stateModel);
    runner.run();

    Assert.assertEquals(runner.waitTillDone().getStatus(), TaskResult.Status.COMPLETED);
    verify(manager, times(2)).getHelixDataAccessor();
    verify(stateModel, never()).updateState(anyString());
    verify(accessor, never()).updateProperty(any(PropertyKey.class), any(CurrentState.class));
  }

  private static HelixDataAccessor mockAccessor() {
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    PropertyKey.Builder keyBuilder = mock(PropertyKey.Builder.class);
    PropertyKey key = mock(PropertyKey.class);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.currentState(anyString(), anyString(), anyString())).thenReturn(key);
    when(keyBuilder.taskCurrentState(anyString(), anyString(), anyString())).thenReturn(key);
    when(accessor.updateProperty(eq(key), any(CurrentState.class))).thenReturn(true);
    return accessor;
  }

  private static Task completedTask() {
    return new Task() {
      @Override
      public TaskResult run() {
        return new TaskResult(TaskResult.Status.COMPLETED, "done");
      }

      @Override
      public void cancel() {
      }
    };
  }
}
