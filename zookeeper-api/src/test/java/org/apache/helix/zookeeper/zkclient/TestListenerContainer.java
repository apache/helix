package org.apache.helix.zookeeper.zkclient;

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

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestListenerContainer {
  private final Random _random = new Random();

  @Test
  public void basicTest() {
    ListenerContainer<String> container = new ListenerContainer<>();
    String key1 = "key-1";
    String key2 = "key-2";
    container.addListener(key1, "test1", false);
    container.addListener(key1, "test2", false);
    container.addListener(key2, "test3", false);
    container.addListener(key2, "test4", false);
    container.addListener(key2, "test5", false);
    container.addListener(key2, "test5", true);
    container.addListener(key1, "test1", true);
    container.addListener(key1, "test4", true);
    container.addListener(key1, "test5", true);
    Assert.assertEquals(container.getOnetimeListener().get(key1).size(), 2);
    Assert.assertEquals(container.getPersistentListener().get(key1).size(), 3);
    Assert.assertEquals(container.getOnetimeListener().get(key2).size(), 3);
    Assert.assertTrue(container.isEmpty("not-a-key"));
    // test remove listeners
    container.removeListener(key2, "test4", false);
    container.removeListener(key2, "test5");
    Assert.assertEquals(container.getOnetimeListener().get(key2).size(), 1);
    Assert.assertNull(container.getPersistentListener().get(key2));
    // test consume listeners
    AtomicInteger invocationCnt = new AtomicInteger(0);
    container.consumeListeners(key1, s -> invocationCnt.incrementAndGet());
    Assert.assertEquals(invocationCnt.get(), 4);
    Assert.assertNull(container.getOnetimeListener().get(key1));
    Assert.assertEquals(container.getPersistentListener().get(key1).size(), 3);
    Assert.assertEquals(container.getOnetimeListener().get(key2).size(), 1);
  }

  @Test(invocationCount = 10)
  public void concurrentTest() throws InterruptedException {
    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(10);
    ListenerContainer<String> container = new ListenerContainer<>();
    AtomicInteger counter0 = new AtomicInteger(0);
    AtomicInteger counter1 = new AtomicInteger(0);

    int numListeners = 10;
    int numItr = 200;
    for (int i = 0; i < numListeners; i++) {
      for (int k = 0; k < 2; k++) {
        container.addListener("key-" + k, "test-non-persist-" + i, false);
        container.addListener("key-" + k, "test-persist-" + i, true);
      }
    }

    int k = 10; // dispatch a WriteTask every k iteration
    for (int i = 0; i < numItr; i++) {
      // setup read and write tasks with 10 : 1 proportion
      fixedThreadPool.submit(new ReadTask(container, "key-0", counter0));
      fixedThreadPool.submit(new ReadTask(container, "key-1", counter1));
      if (i % k == 0) {
        fixedThreadPool.submit(new WriteTask(container, "key-0", "test-non-persist-additional-" + i, false));
      }
    }
    fixedThreadPool.shutdown();
    Assert.assertTrue(fixedThreadPool.awaitTermination(30, TimeUnit.SECONDS));
    Assert.assertEquals(counter0.get(), numItr * numListeners + numListeners + numItr / k);
    Assert.assertEquals(counter1.get(), numItr * numListeners + numListeners);
  }

  class ReadTask implements Runnable {
    private final  ListenerContainer<String> _listenerContainer;
    private final String _key;
    private final AtomicInteger _counter;

    ReadTask(ListenerContainer<String> container, String key, AtomicInteger counter) {
      _listenerContainer = container;
      _key = key;
      _counter = counter;
    }

    @Override
    public void run() {
      _listenerContainer.consumeListeners(_key, t -> {
        _counter.incrementAndGet();
        try {
          Thread.sleep(10 + _random.nextInt(10));
        } catch (InterruptedException ignored) { }
      });
    }
  }

  class WriteTask implements Runnable {
    private final ListenerContainer<String> _listenerContainer;
    private final String _key;
    private final String _name;
    private final boolean _persist;

    WriteTask(ListenerContainer<String> container, String key, String name, boolean persist) {
      _listenerContainer = container;
      _key = key;
      _name = name;
      _persist = persist;
    }

    @Override
    public void run() {
      _listenerContainer.addListener(_key, _name, _persist);
    }
  }
}
