package org.apache.helix.ipc.netty;

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

import static org.apache.helix.ipc.netty.NettyHelixIPCUtils.*;

import com.codahale.metrics.Meter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.helix.ipc.HelixIPCCallback;
import org.apache.helix.resolver.HelixMessageScope;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

@ChannelHandler.Sharable
public class NettyHelixIPCCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final String instanceName;
  private final ConcurrentMap<Integer, HelixIPCCallback> callbacks;
  private final Meter statRxMsg;
  private final Meter statRxBytes;

  public NettyHelixIPCCallbackHandler(String instanceName,
                                      ConcurrentMap<Integer, HelixIPCCallback> callbacks,
                                      Meter statRxMsg,
                                      Meter statRxBytes) {
    super(false); // we will manage reference
    this.instanceName = instanceName;
    this.callbacks = callbacks;
    this.statRxMsg = statRxMsg;
    this.statRxBytes = statRxBytes;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf byteBuf) throws Exception {
    try {
      // Message length
      int messageLength = byteBuf.readInt();

      // Message version
      @SuppressWarnings("unused")
      int messageVersion = byteBuf.readInt();

      // Message type
      int messageType = byteBuf.readInt();

      // Message ID
      UUID messageId = new UUID(byteBuf.readLong(), byteBuf.readLong());

      // Cluster
      int clusterSize = byteBuf.readInt();
      checkLength("clusterSize", clusterSize, messageLength);
      String clusterName = toNonEmptyString(clusterSize, byteBuf);

      // Resource
      int resourceSize = byteBuf.readInt();
      checkLength("resourceSize", resourceSize, messageLength);
      String resourceName = toNonEmptyString(resourceSize, byteBuf);

      // Partition
      int partitionSize = byteBuf.readInt();
      checkLength("partitionSize", partitionSize, messageLength);
      String partitionName = toNonEmptyString(partitionSize, byteBuf);

      // State
      int stateSize = byteBuf.readInt();
      checkLength("stateSize", stateSize, messageLength);
      String state = toNonEmptyString(stateSize, byteBuf);

      // Source instance
      int srcInstanceSize = byteBuf.readInt();
      checkLength("srcInstanceSize", srcInstanceSize, messageLength);
      String srcInstance = toNonEmptyString(srcInstanceSize, byteBuf);

      // Destination instance
      int dstInstanceSize = byteBuf.readInt();
      checkLength("dstInstanceSize", dstInstanceSize, messageLength);
      String dstInstance = toNonEmptyString(dstInstanceSize, byteBuf);

      // Message
      int messageSize = byteBuf.readInt();
      ByteBuf message = byteBuf.slice(byteBuf.readerIndex(), messageSize);

      // Error check
      if (dstInstance == null) {
        throw new IllegalStateException("Received message addressed to null destination from "
            + srcInstance);
      } else if (!dstInstance.equals(instanceName)) {
        throw new IllegalStateException(instanceName
            + " received message addressed to " + dstInstance + " from " + srcInstance);
      } else if (callbacks.get(messageType) == null) {
        throw new IllegalStateException("No callback registered for message type " + messageType);
      }

      // Build scope
      HelixMessageScope scope =
          new HelixMessageScope.Builder().cluster(clusterName).resource(resourceName)
              .partition(partitionName).state(state).sourceInstance(srcInstance).build();

      // Get callback
      HelixIPCCallback callback = callbacks.get(messageType);
      if (callback == null) {
        throw new IllegalStateException("No callback registered for message type " + messageType);
      }

      // Handle callback
      callback.onMessage(scope, messageId, message);

      // Stats
      statRxMsg.mark();
      statRxBytes.mark(messageLength);
    } finally {
      byteBuf.release();
    }

  }
}
