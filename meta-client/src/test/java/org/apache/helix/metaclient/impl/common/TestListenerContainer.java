package org.apache.helix.metaclient.impl.common;

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

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestListenerContainer {

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
    container.removeListener(key2, "test4", false);
    container.removeListener(key2, "test5");
    Assert.assertEquals(container.getOnetimeListener().get(key2).size(), 1);
    Assert.assertNull(container.getPersistentListener().get(key2));
    AtomicInteger invocationCnt = new AtomicInteger(0);
    container.consumeListeners(key1, s -> invocationCnt.incrementAndGet());
    Assert.assertEquals(invocationCnt.get(), 4);
    Assert.assertNull(container.getOnetimeListener().get(key1));
    Assert.assertEquals(container.getPersistentListener().get(key1).size(), 3);
    Assert.assertEquals(container.getOnetimeListener().get(key2).size(), 1);
  }
}
