package org.apache.helix.metaclient.puppy;

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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to manage the lifecycle of a set of _puppies.
 */
public class PuppyManager {
  private final List<AbstractPuppy> _puppies = new ArrayList<>();
  private final ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();

  public PuppyManager() {
  }

  public void addPuppy(AbstractPuppy puppy) {
    _puppies.add(puppy);
  }

  public List<AbstractPuppy> getPuppies() {
    return _puppies;
  }

  public void start(long timeoutInSeconds) {
    for (AbstractPuppy puppy : _puppies) {
      EXECUTOR_SERVICE.submit(puppy);
    }

    try {
      EXECUTOR_SERVICE.awaitTermination(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignore
    }

    stop();
  }

  public void stop() {
    EXECUTOR_SERVICE.shutdownNow();
  }
}

