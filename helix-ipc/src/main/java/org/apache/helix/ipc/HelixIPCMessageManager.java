package org.apache.helix.ipc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.netty.buffer.ByteBuf;
import org.apache.helix.resolver.HelixAddress;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper around a base IPC service that manages message retries / timeouts.
 * <p>
 * This class manages retries and timeouts, and can be used in the same way as a
 * {@link org.apache.helix.ipc.HelixIPCService}.
 * </p>
 * <p>
 * A message will be sent until the max number of retries has been reached (i.e. timed out), or it
 * is acknowledged by the recipient. If the max number of retries is -1, it will be retried forever.
 * </p>
 * <p>
 * A callback should be registered for every acknowledgement message type associated with any
 * original message type sent by this class.
 * </p>
 * <p>
 * For example, consider we have the two message types defined: DATA_REQ = 1, DATA_ACK = 2. One
 * would do the following:
 * 
 * <pre>
 * messageManager.registerCallback(DATA_ACK, new HelixIPCCallback() {
 *   public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
 *     // Process ACK
 *   }
 * 
 *   public void onError(HelixMessageScope scope, UUID messageId, Throwable cause) {
 *     // Message error or timeout
 *   }
 * });
 * 
 * messageManager.send(destinations, DATA_REQ, messageId, data);
 * </pre>
 * 
 * </p>
 * <p>
 * In send, we note messageId, and retry until we get a DATA_ACK for the same messageId. The
 * callback registered with the message manager will only be called once, even if the message is
 * acknowledged several times.
 * </p>
 */
public class HelixIPCMessageManager implements HelixIPCService {

  private static final Logger LOG = Logger.getLogger(HelixIPCMessageManager.class);

  private final ScheduledExecutorService scheduler;
  private final HelixIPCService baseIpcService;
  private final long messageTimeoutMillis;
  private final int maxNumRetries;
  private final ConcurrentMap<UUID, Boolean> pendingMessages;
  private final ConcurrentMap<UUID, AtomicInteger> retriesLeft;
  private final ConcurrentMap<UUID, ByteBuf> messageBuffers;
  private final ConcurrentMap<Integer, HelixIPCCallback> callbacks;

  public HelixIPCMessageManager(ScheduledExecutorService scheduler, HelixIPCService baseIpcService,
      long messageTimeoutMillis, int maxNumRetries) {
    this.scheduler = scheduler;
    this.baseIpcService = baseIpcService;
    this.maxNumRetries = maxNumRetries;
    this.messageTimeoutMillis = messageTimeoutMillis;
    this.pendingMessages = new ConcurrentHashMap<UUID, Boolean>();
    this.retriesLeft = new ConcurrentHashMap<UUID, AtomicInteger>();
    this.messageBuffers = new ConcurrentHashMap<UUID, ByteBuf>();
    this.callbacks = new ConcurrentHashMap<Integer, HelixIPCCallback>();
  }

  @Override
  public void start() throws Exception {
    baseIpcService.start();
  }

  @Override
  public void shutdown() throws Exception {
    baseIpcService.shutdown();
  }

  @Override
  public void send(final HelixAddress destination, final int messageType, final UUID messageId,
      final ByteBuf message) {
    // State
    pendingMessages.put(messageId, true);
    retriesLeft.putIfAbsent(messageId, new AtomicInteger(maxNumRetries));
    messageBuffers.put(messageId, message);

    // Will free it when we've finally received response
    message.retain();

    // Send initial message
    baseIpcService.send(destination, messageType, messageId, message);

    // Retries
    scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        Boolean isPending = pendingMessages.get(messageId);
        AtomicInteger numLeft = retriesLeft.get(messageId);
        if (numLeft != null && isPending != null && isPending) {
          if (numLeft.decrementAndGet() > 0 || maxNumRetries == -1) {
            // n.b. will schedule another retry
            send(destination, messageType, messageId, message);
          } else {
            LOG.warn("Message " + messageId + " timed out after " + maxNumRetries + " retries");
          }
        }
      }
    }, messageTimeoutMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void registerCallback(final int messageType, final HelixIPCCallback callback) {

    // This callback will first check if the message is pending, then delegate to the provided
    // callback if it has not yet done so.
    HelixIPCCallback wrappedCallback = new HelixIPCCallback() {
      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        if (pendingMessages.replace(messageId, true, false)) {
          pendingMessages.remove(messageId);
          ByteBuf originalMessage = messageBuffers.remove(messageId);
          if (originalMessage != null) {
            originalMessage.release();
          }
          retriesLeft.remove(messageId);
          callback.onMessage(scope, messageId, message);
        }
      }
    };

    callbacks.put(messageType, wrappedCallback);
    baseIpcService.registerCallback(messageType, wrappedCallback);
  }
}
