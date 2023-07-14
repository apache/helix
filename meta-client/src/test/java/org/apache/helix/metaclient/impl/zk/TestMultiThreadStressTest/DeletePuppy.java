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
import org.apache.helix.metaclient.puppy.AbstractPuppy;
import org.apache.helix.metaclient.puppy.PuppySpec;

import java.util.Random;

public class DeletePuppy extends AbstractPuppy {

  private Random random;

  public DeletePuppy(MetaClientInterface<String> metaclient, PuppySpec puppySpec) {
    super(metaclient, puppySpec);
    random = new Random();
  }

  @Override
  protected void bark() {
    int randomNumber = random.nextInt(_puppySpec.getNumberDiffPaths());
    if (shouldIntroduceError()) {
      try {
        _metaclient.delete("invalid");
        _unhandledErrorCounter++;
      } catch (IllegalArgumentException e) {
        System.out.println(Thread.currentThread().getName() + " intentionally deleted an invalid path.");
      }
    } else {
      System.out.println(Thread.currentThread().getName() + " is attempting to delete node: " + randomNumber);
      if (_metaclient.delete("/test/" + randomNumber)) {
        System.out.println(Thread.currentThread().getName() + " successfully deleted node " + randomNumber + " at time: " + System.currentTimeMillis());
        _eventChangeCounterMap.put(String.valueOf(randomNumber), _eventChangeCounterMap.getOrDefault(String.valueOf(randomNumber), 0) + 1);
      } else {
        System.out.println(Thread.currentThread().getName() + " failed to delete node " + randomNumber + ", it does not exist");
      }
    }
  }

  @Override
  protected void cleanup() {
    _metaclient.recursiveDelete("/test");
  }

  private boolean shouldIntroduceError() {
    return random.nextFloat() < _puppySpec.getErrorRate();
  }
}