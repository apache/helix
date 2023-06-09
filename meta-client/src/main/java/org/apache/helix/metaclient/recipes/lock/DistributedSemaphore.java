package org.apache.helix.metaclient.recipes.lock;

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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.metaclient.api.MetaClientInterface;
import org.apache.helix.metaclient.datamodel.DataRecord;
import org.apache.helix.metaclient.factories.MetaClientConfig;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class DistributedSemaphore {

  /**
   * Create a distributed semaphore client with the given configuration.
   * @param config configuration of the client
   */
  public DistributedSemaphore(MetaClientConfig config) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Connect to an existing distributed semaphore client.
   * @param client client to connect to
   */
  public DistributedSemaphore(MetaClientInterface<DataRecord> client) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Create a distributed semaphore with the given path and capacity.
   * @param path path of the semaphore
   * @param capacity capacity of the semaphore
   */
  public void createSemaphore(String path, int capacity) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Connect to an existing distributed semaphore.
   * @param path path of the semaphore
   */
  public void connectSemaphore(String path) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Acquire a permit. If no permit is available, log error and return null.
   * @return a permit
   */
  public Permit acquire() {
    throw new NotImplementedException("Not implemented yet.");
  }


  /**
   * Try to acquire multiple permits. If not enough permits are available, log error and return null.
   * @param count number of permits to acquire
   * @return a collection of permits
   */
  public Collection<Permit> acquire(int count) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Try to acquire a permit. If no enough permit is available, wait for a specific time or return when it was able to acquire.
   * If timeout <=0, then return immediately when not able to acquire.
   * @param count number of permits to acquire
   * @param timeout time to wait
   * @param unit time unit
   * @return a collection of permits
   */
  public Collection<Permit> acquire(int count, long timeout, TimeUnit unit) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Get the remaining capacity of the semaphore
   * @return remaining capacity
   */
  public long getRemainingCapacity() {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Get the semaphore data record
   * @return semaphore data record
   */
  private DataRecord getSemaphore() {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Return a permit. If the permit is already returned, log and return void.
   */
  public void returnPermit(Permit permit) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Return a collection of permits. If a permit in that collection is already returned,
   * log and return void.
   */
  public void returnAllPermits(Collection<Permit> permits) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Retrieve a permit from the semaphore data record.
   * @param path path of the permit
   * @return a permit
   */
  private Permit retrievePermit(String path) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Update the remaining capacity of the semaphore after acquiring a permit.
   * @param count number of permits to acquire
   */
  private void updateAcquirePermit(int count) {
    throw new NotImplementedException("Not implemented yet.");
  }

  /**
   * Update the remaining capacity of the semaphore after returning a permit.
   */
  private void updateReturnPermit() {
    throw new NotImplementedException("Not implemented yet.");
  }
}
