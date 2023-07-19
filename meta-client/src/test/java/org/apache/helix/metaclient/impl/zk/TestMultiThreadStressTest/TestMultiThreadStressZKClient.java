package org.apache.helix.metaclient.impl.zk.TestMultiThreadStressTest;

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

import org.apache.helix.metaclient.api.ChildChangeListener;
import org.apache.helix.metaclient.impl.zk.ZkMetaClient;
import org.apache.helix.metaclient.impl.zk.ZkMetaClientTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMultiThreadStressZKClient extends ZkMetaClientTestBase {

  private ZkMetaClient<String> _zkMetaClient;
  private final String zkParentKey = "/test";

  @BeforeTest
  private void setUp() {
    this._zkMetaClient = createZkMetaClient();
    this._zkMetaClient.connect();
  }

  @Test
  public void testCreatePuppy() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    CreatePuppy createPuppy2 = new CreatePuppy(_zkMetaClient, puppySpec);
    CreatePuppy createPuppy3 = new CreatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(createPuppy2);
    puppyManager.addPuppy(createPuppy3);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testDeletePuppy() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    DeletePuppy deletePuppy = new DeletePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(deletePuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testGetPuppy() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    GetPuppy getPuppy = new GetPuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(getPuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testSetPuppy() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    SetPuppy setPuppy = new SetPuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(setPuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testUpdatePuppy() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    UpdatePuppy updatePuppy = new UpdatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(updatePuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testCrudPuppies() {
    _zkMetaClient.create(zkParentKey, "test");

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    GetPuppy getPuppy = new GetPuppy(_zkMetaClient, puppySpec);
    DeletePuppy deletePuppy = new DeletePuppy(_zkMetaClient, puppySpec);
    SetPuppy setPuppy = new SetPuppy(_zkMetaClient, puppySpec);
    UpdatePuppy updatePuppy = new UpdatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(getPuppy);
    puppyManager.addPuppy(deletePuppy);
    puppyManager.addPuppy(setPuppy);
    puppyManager.addPuppy(updatePuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, null);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
  }

  @Test
  public void testBasicParentListenerPuppy() {
    _zkMetaClient.create(zkParentKey, "test");
    AtomicInteger globalChildChangeCounter = new AtomicInteger();
    ChildChangeListener childChangeListener = (changedPath, changeType) -> globalChildChangeCounter.addAndGet(1);

    _zkMetaClient.subscribeChildChanges(zkParentKey, childChangeListener, false);

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);

    long timeoutInSeconds = 10; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, globalChildChangeCounter);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    _zkMetaClient.unsubscribeChildChanges(zkParentKey, childChangeListener);
  }

  @Test
  public void testComplexParentListenerPuppy() {
    _zkMetaClient.create(zkParentKey, "test");
    // Global counter for all child changes
    AtomicInteger globalChildChangeCounter = new AtomicInteger();
    ChildChangeListener childChangeListener = (changedPath, changeType) -> globalChildChangeCounter.addAndGet(1);

    _zkMetaClient.subscribeChildChanges(zkParentKey, childChangeListener, false);

    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 5);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    GetPuppy getPuppy = new GetPuppy(_zkMetaClient, puppySpec);
    DeletePuppy deletePuppy = new DeletePuppy(_zkMetaClient, puppySpec);
    SetPuppy setPuppy = new SetPuppy(_zkMetaClient, puppySpec);
    UpdatePuppy updatePuppy = new UpdatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(getPuppy);
    puppyManager.addPuppy(deletePuppy);
    puppyManager.addPuppy(setPuppy);
    puppyManager.addPuppy(updatePuppy);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    assertNoExceptions(puppyManager, globalChildChangeCounter);

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    _zkMetaClient.unsubscribeChildChanges(zkParentKey, childChangeListener);
  }

  @Test
  public void testChildListenerPuppy() {
    _zkMetaClient.create(zkParentKey, "test");
    // Setting num diff paths to 3 until we find a better way of scaling listeners.
    PuppySpec puppySpec = new PuppySpec(PuppyMode.Repeat, 0.2f, new ExecDelay(5000, 0.1f), 3);
    CreatePuppy createPuppy = new CreatePuppy(_zkMetaClient, puppySpec);
    GetPuppy getPuppy = new GetPuppy(_zkMetaClient, puppySpec);
    DeletePuppy deletePuppy = new DeletePuppy(_zkMetaClient, puppySpec);
    SetPuppy setPuppy = new SetPuppy(_zkMetaClient, puppySpec);
    UpdatePuppy updatePuppy = new UpdatePuppy(_zkMetaClient, puppySpec);

    PuppyManager puppyManager = new PuppyManager();
    puppyManager.addPuppy(createPuppy);
    puppyManager.addPuppy(getPuppy);
    puppyManager.addPuppy(deletePuppy);
    puppyManager.addPuppy(setPuppy);
    puppyManager.addPuppy(updatePuppy);

    // Create a child listener for each child defined in number diff paths in puppyspec.
    // TODO: Make this a parameter for a loop.
    AtomicInteger childChangeCounter0 = new AtomicInteger();
    ChildChangeListener childChangeListener0 = (changedPath, changeType) ->
        childChangeCounter0.addAndGet(1);
    _zkMetaClient.subscribeChildChanges("/test/0", childChangeListener0, false);

    AtomicInteger childChangeCounter1 = new AtomicInteger();
    ChildChangeListener childChangeListener1 = (changedPath, changeType) ->
        childChangeCounter1.addAndGet(1);
    _zkMetaClient.subscribeChildChanges("/test/1", childChangeListener1, false);

    AtomicInteger childChangeCounter2 = new AtomicInteger();
    ChildChangeListener childChangeListener2 = (changedPath, changeType) ->
        childChangeCounter2.addAndGet(1);
    _zkMetaClient.subscribeChildChanges("/test/2", childChangeListener2, false);

    long timeoutInSeconds = 60; // Set the desired timeout duration

    puppyManager.start(timeoutInSeconds);

    // Setting globalChangeCounter to null. Each child listener has its own counter.
    // Check is done in the test method.
    assertNoExceptions(puppyManager, null);

    // Add all event changes from all puppies and compare with child change listener
    // Inner merged by path
    Map<String, Integer> mergedEventChangeCounterMap = new HashMap<>();
    for (AbstractPuppy puppy : puppyManager.getPuppies()) {
      puppy.eventChangeCounterMap.forEach((key, value) -> {
        if (mergedEventChangeCounterMap.containsKey(key)) {
          mergedEventChangeCounterMap.put(key, mergedEventChangeCounterMap.get(key) + value);
        } else {
          mergedEventChangeCounterMap.put(key, value);
        }
      });
    }

    System.out.println("Merged event change counter map: " + mergedEventChangeCounterMap);
    System.out.println("Child change counter 0: " + childChangeCounter0);
    System.out.println("Child change counter 1: " + childChangeCounter1);
    System.out.println("Child change counter 2: " + childChangeCounter2);
    Assert.assertEquals(childChangeCounter0.get(), mergedEventChangeCounterMap.getOrDefault("0", 0).intValue());
    Assert.assertEquals(childChangeCounter1.get(), mergedEventChangeCounterMap.getOrDefault("1", 0).intValue());
    Assert.assertEquals(childChangeCounter2.get(), mergedEventChangeCounterMap.getOrDefault("2", 0).intValue());

    // cleanup
    _zkMetaClient.recursiveDelete(zkParentKey);
    Assert.assertEquals(_zkMetaClient.countDirectChildren(zkParentKey), 0);
    _zkMetaClient.unsubscribeChildChanges("/test/0", childChangeListener0);
    _zkMetaClient.unsubscribeChildChanges("/test/1", childChangeListener1);
    _zkMetaClient.unsubscribeChildChanges("/test/2", childChangeListener2);
  }

  private void assertNoExceptions(PuppyManager puppyManager, AtomicInteger globalChangeCounter) {
    int totalUnhandledErrors = 0;
    int totalEventChanges = 0;

    // Add all change counters and compare with event change listener
    for (AbstractPuppy puppy : puppyManager.getPuppies()) {
      AtomicInteger totalHandledErrors = new AtomicInteger();
      puppy.eventChangeCounterMap.forEach((key, value) -> {
        totalHandledErrors.addAndGet(value);
      });

      System.out.println("Change counter: " + totalHandledErrors + " for " + puppy.getClass());
      System.out.println("Error counter: " + puppy.unhandledErrorCounter + " for " + puppy.getClass());
      totalUnhandledErrors += puppy.unhandledErrorCounter;
      totalEventChanges += totalHandledErrors.get();
    }

    // Assert no unhandled (unexpected) exceptions and that the child change listener placed on
    // test parent node (/test) caught all successful changes that were recorded by each puppy
    Assert.assertEquals(totalUnhandledErrors, 0);

    // Assert that the global change counter matches the total number of events recorded by each puppy
    if (globalChangeCounter != null) {
      Assert.assertEquals(totalEventChanges, globalChangeCounter.get());
    }
  }
}
