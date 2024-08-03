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
import org.apache.helix.metaclient.exception.MetaClientNoNodeException;
import org.apache.helix.metaclient.puppy.AbstractPuppy;
import org.apache.helix.metaclient.puppy.PuppySpec;

import java.util.Random;

public class SetPuppy extends AbstractPuppy {

  private final Random _random;
  private final String _parentPath = "/test";

  public SetPuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec, String parentPath) {
    super(metaclient, puppySpec, parentPath);
    _random = new Random();
  }

  @Override
  protected void bark() {
    int randomNumber = _random.nextInt(_puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      try {
        _metaclient.set("invalid", "test", -1);
      } catch (IllegalArgumentException e) {
        System.out.println(Thread.currentThread().getName() + " intentionally called set on an invalid path" + " at time: " + System.currentTimeMillis());
      }
    } else {
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to set node: " + randomNumber + " at time: " + System.currentTimeMillis());
        _metaclient.set(_parentPath + "/" + randomNumber, "test", -1);
        _eventChangeCounterMap.put(String.valueOf(randomNumber), _eventChangeCounterMap.getOrDefault(String.valueOf(randomNumber), 0) + 1);
        System.out.println(
            Thread.currentThread().getName() + " successfully set node " + randomNumber + " at time: "
                + System.currentTimeMillis());
      } catch (MetaClientNoNodeException e) {
        System.out.println(Thread.currentThread().getName() + " failed to set node " + randomNumber + " at time: " + System.currentTimeMillis() + ", it does not exist");
      }
    }
  }

  @Override
  protected void cleanup() {
  }

  private boolean shouldIntroduceError() {
    float randomValue = _random.nextFloat();
    return randomValue < _puppySpec.getErrorRate();
  }
}
