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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.helix.common.ClusterEventBlockingQueue;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test {@link ClusterEventBlockingQueue} to ensure that it coalesces events while keeping then in
 * FIFO order.
 */
public class TestClusterEventBlockingQueue {
  @Test
  public void testEventQueue() throws Exception {
    // initialize the queue
    ClusterEventBlockingQueue queue = new ClusterEventBlockingQueue();

    // add an event
    ClusterEvent event1 = new ClusterEvent(ClusterEventType.IdealStateChange);
    queue.put(event1);
    Assert.assertEquals(queue.size(), 1);

    // add an event with a different name
    ClusterEvent event2 = new ClusterEvent(ClusterEventType.ConfigChange);
    queue.put(event2);
    Assert.assertEquals(queue.size(), 2);

    // add an event with the same type as event1 (should not change queue size)
    ClusterEvent newEvent1 = new ClusterEvent(ClusterEventType.IdealStateChange);
    newEvent1.addAttribute("attr", 1);
    queue.put(newEvent1);
    Assert.assertEquals(queue.size(), 2);

    // test peek
    ClusterEvent peeked = queue.peek();
    Assert.assertEquals(peeked.getEventType(), ClusterEventType.IdealStateChange);
    Assert.assertEquals((int) peeked.getAttribute("attr"), 1);
    Assert.assertEquals(queue.size(), 2);

    // test take the head
    ListeningExecutorService service =
        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    ClusterEvent takenEvent1 = safeTake(queue, service);
    Assert.assertEquals(takenEvent1.getEventType(), ClusterEventType.IdealStateChange);
    Assert.assertEquals((int) takenEvent1.getAttribute("attr"), 1);
    Assert.assertEquals(queue.size(), 1);

    // test take the tail
    ClusterEvent takenEvent2 = safeTake(queue, service);
    Assert.assertEquals(takenEvent2.getEventType(), ClusterEventType.ConfigChange);
    Assert.assertEquals(queue.size(), 0);
  }

  private ClusterEvent safeTake(final ClusterEventBlockingQueue queue,
      final ListeningExecutorService service) throws InterruptedException, ExecutionException,
      TimeoutException {
    // the take() in ClusterEventBlockingQueue will wait indefinitely
    // for this test, stop waiting after 30 seconds
    ListenableFuture<ClusterEvent> future = service.submit(new Callable<ClusterEvent>() {
      @Override
      public ClusterEvent call() throws InterruptedException {
        return queue.take();
      }
    });
    ClusterEvent event = future.get(30, TimeUnit.SECONDS);
    return event;
  }
}
