package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;


public class TestClusterEvent {

  @Test
  public void testSimplePutAndGet() {
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    AssertJUnit.assertEquals(event.getEventType(), ClusterEventType.Unknown);
    event.addAttribute("attr1", "value");
    AssertJUnit.assertEquals(event.getAttribute("attr1"), "value");
  }

  @Test
  public void testThreadSafeClone() throws InterruptedException {
    String clusterName = "TestCluster";
    ClusterEvent event = new ClusterEvent(clusterName, ClusterEventType.Unknown, "testId");
    for (int i = 0; i < 100; i++) {
      event.addAttribute(String.valueOf(i), i);
    }
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      threads.add(new Thread(() -> {
        String threadName = Thread.currentThread().getName();
        event.addAttribute(threadName, threadName);
      }));
    }
    threads.forEach(Thread::start);
    ClusterEvent clonedEvent;
    try {
      clonedEvent = event.clone("cloneId");
      Assert.assertEquals(clonedEvent.getClusterName(), clusterName);
      Assert.assertEquals(clonedEvent.getEventId(), "cloneId");
    } catch (ConcurrentModificationException e) {
      Assert.fail("Didn't expect any exception to occur", e);
    }

    for (Thread t : threads) {
      t.join();
    }
  }
}
