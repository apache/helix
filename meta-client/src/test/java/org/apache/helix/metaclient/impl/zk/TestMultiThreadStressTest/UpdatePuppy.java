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

public class UpdatePuppy extends AbstractPuppy {

  private final Random _random;

  public UpdatePuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec, String parentPath) {
    super(metaclient, puppySpec, parentPath);
    _random = new Random();
  }

  @Override
  protected void bark() throws Exception {
    int randomNumber = _random.nextInt(_puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      try {
        _metaclient.update("invalid", (data) -> "foo");
      } catch (IllegalArgumentException e) {
        System.out.println(Thread.currentThread().getName() + " intentionally tried to update an invalid path" + " at time: " + System.currentTimeMillis());
      }
    } else {
      try {
        System.out.println(Thread.currentThread().getName() + " is attempting to update node: " + randomNumber + " at time: " + System.currentTimeMillis());
        _metaclient.update(_parentPath + "/" + randomNumber, (data) -> "foo");
        _eventChangeCounterMap.put(String.valueOf(randomNumber), _eventChangeCounterMap.getOrDefault(String.valueOf(randomNumber), 0) + 1);
        System.out.println(Thread.currentThread().getName() + " successfully updated node " + randomNumber + " at time: "
            + System.currentTimeMillis());
      } catch (MetaClientNoNodeException e) {
        System.out.println(Thread.currentThread().getName() + " failed to update node " + randomNumber + " at time: " + System.currentTimeMillis() + ", it does not exist");
      } catch (IllegalArgumentException e) {
        if (!e.getMessage().equals("Can not subscribe one time watcher when ZkClient is using PersistWatcher")) {
          throw e;
        }
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
