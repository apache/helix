package org.apache.helix.controller.pipeline;

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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractBaseStage implements Stage {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractBaseStage.class);

  protected String _eventId;

  @Override
  public void init(StageContext context) {

  }

  @Override
  public void preProcess() {
    // TODO Auto-generated method stub

  }

  @Override
  public void process(ClusterEvent event) throws Exception {

  }

  @Override
  public void postProcess() {

  }

  @Override
  public void release() {

  }

  @Override
  public String getStageName() {
    // default stage name will be the class name
    String className = this.getClass().getSimpleName();
    return className;
  }

  /**
   * Tells whether the controller's ZooKeeper session has changed since the event was created, in
   * which case the event's data is stale: the session expired while the event was queued or being
   * processed, and another controller may already lead the cluster. Stages must not act on such an
   * event, in particular not write cluster-wide state (messages, views, persisted assignments).
   * <p>
   * Every event the controller creates in production carries the leader session it was created in
   * ({@link AttributeName#EVENT_SESSION}); events without that attribute (only built by tests) are
   * never stale. The check does not wait for a disconnected manager to reconnect: while the manager
   * is disconnected the session is unknown (it may still be the same one after a reconnect), so the
   * event is not reported as stale and processing goes on as before, relying on the ZkClient retry
   * logic and on {@code MessageDispatchStage}'s blocking session check that guards message sending.
   * The manager's session id is updated when it handles the new session, so an event is recognized
   * as stale from that point on; that is when the stale pipelines observed in practice resume, as
   * they were blocked on ZooKeeper reads during the expiry.
   * @param event the event being processed
   * @return true if the event's manager is connected and its session differs from the event's
   *         session
   */
  static boolean isEventSessionStale(ClusterEvent event) {
    Optional<String> eventSession = event.getAttribute(AttributeName.EVENT_SESSION.name());
    HelixManager manager = event.getAttribute(AttributeName.helixmanager.name());
    if (eventSession == null || !eventSession.isPresent() || manager == null) {
      // No session recorded: either a test-built event, or the controller was not leader when
      // the event was created. GenericHelixController#handleEvent already filters the latter.
      return false;
    }
    try {
      return manager.isConnected() && !eventSession.get().equals(manager.getSessionId());
    } catch (HelixException e) {
      // The manager got disconnected in between; the session is unknown, not known to be stale.
      LOG.debug("Cannot determine the session of event {}: manager is not connected",
          event.getEventId(), e);
      return false;
    }
  }

  /**
   * @return the session the event was created in, for log messages
   */
  static String describeEventSession(ClusterEvent event) {
    Optional<String> eventSession = event.getAttribute(AttributeName.EVENT_SESSION.name());
    return eventSession == null ? "NOT_PRESENT" : eventSession.orElse("NOT_PRESENT");
  }

  public static <T> Future asyncExecute(ExecutorService service, Callable<T> task) {
    if (service != null) {
      return service.submit(task);
    }
    return null;
  }

  protected DedupEventProcessor<String, Runnable> getAsyncWorkerFromClusterEvent(ClusterEvent event,
      AsyncWorkerType workerType) {
    Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> workerPool =
        event.getAttribute(AttributeName.AsyncFIFOWorkerPool.name());
    if (workerPool != null) {
      if (workerPool.containsKey(workerType)) {
        return workerPool.get(workerType);
      }
    }
    return null;
  }
}
