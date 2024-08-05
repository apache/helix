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
import org.apache.helix.metaclient.exception.MetaClientNodeExistsException;
import org.apache.helix.metaclient.puppy.AbstractPuppy;
import org.apache.helix.metaclient.puppy.PuppySpec;

import java.util.Random;

public class CreatePuppy extends AbstractPuppy {

  private final Random _random;

  public CreatePuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec, String parentPath) {
    super(metaclient, puppySpec, parentPath);
    _random = new Random();
  }

  @Override
  protected void bark() {
    // Implement the chaos logic for creating nodes
    int randomNumber = _random.nextInt(_puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      try {
        // Simulate an error by creating an invalid path
        _metaclient.create("invalid", "test");
      } catch (IllegalArgumentException e) { // Catch invalid exception
        System.out.println(Thread.currentThread().getName() + " tried to create an invalid path" + " at time: " + System.currentTimeMillis());
        // Expected exception
      }
    } else {
      // Normal behavior - create a new node
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to create node: " + randomNumber + " at time: " + System.currentTimeMillis());
        _metaclient.create(_parentPath + "/" + randomNumber,"test");
        System.out.println(Thread.currentThread().getName() + " successfully created node " + randomNumber + " at time: " + System.currentTimeMillis());
        _eventChangeCounterMap.put(String.valueOf(randomNumber), _eventChangeCounterMap.getOrDefault(String.valueOf(randomNumber), 0) + 1);
      } catch (MetaClientNodeExistsException e) {
        // Expected exception
        System.out.println(Thread.currentThread().getName() + " failed to create node " + randomNumber + " at time: " + System.currentTimeMillis() + ", it already exists");
      }
    }
  }

  @Override
  protected void cleanup() {
    // Cleanup logic in test case
  }

  private boolean shouldIntroduceError() {
    float randomValue = _random.nextFloat();
    return randomValue < _puppySpec.getErrorRate();
  }
}



