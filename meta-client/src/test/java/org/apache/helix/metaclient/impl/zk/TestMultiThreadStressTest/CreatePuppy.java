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

import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.exception.MetaClientException;

import java.util.Random;

public class CreatePuppy extends AbstractPuppy {

  public CreatePuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    super(metaclient, puppySpec);
  }

  @Override
  protected void bark() {
    // Implement the chaos logic for creating nodes
    int random = new Random().nextInt(puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      try {
        // Simulate an error by creating an invalid path
        metaclient.create("invalid", "test");
      } catch (MetaClientException e) { // Catch invalid exception
        System.out.println(Thread.currentThread().getName() + " tried to create an invalid path.");
        // Expected exception
      }
    } else {
      // Normal behavior - create a new node
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to create node: " + random);
        metaclient.create("/test/" + random,"test");
        System.out.println(Thread.currentThread().getName() + " successfully created node " + random + " at time: " + System.currentTimeMillis());
        eventChangeCounterMap.put(String.valueOf(random), eventChangeCounterMap.getOrDefault(String.valueOf(random), 0) + 1);
      } catch (MetaClientException e) { // TODO: Catch more specific
        // Expected exception
        System.out.println(Thread.currentThread().getName() + " failed to create node " + random + ", it already exists");
      }
    }
  }

  @Override
  protected void cleanup() {
    // Implement the recovery logic by deleting the created documents
    metaclient.recursiveDelete("/test");
  }

  private boolean shouldIntroduceError() {
    Random random = new Random();
    float randomValue = random.nextFloat();
    return randomValue < puppySpec.getErrorRate();
  }
}


