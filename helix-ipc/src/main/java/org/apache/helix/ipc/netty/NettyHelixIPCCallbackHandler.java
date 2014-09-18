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
      int idx = 0;

      // Message length
      int messageLength = byteBuf.readInt();
      idx += 4;

      // Message version
      @SuppressWarnings("unused")
      int messageVersion = byteBuf.readInt();
      idx += 4;

      // Message type
      int messageType = byteBuf.readInt();
      idx += 4;

      // Message ID
      UUID messageId = new UUID(byteBuf.readLong(), byteBuf.readLong());
      idx += 16;

      // Cluster
      byteBuf.readerIndex(idx);
      int clusterSize = byteBuf.readInt();
      idx += 4;
      checkLength("clusterSize", clusterSize, messageLength);
      String clusterName = toNonEmptyString(clusterSize, byteBuf);
      idx += clusterSize;

      // Resource
      byteBuf.readerIndex(idx);
      int resourceSize = byteBuf.readInt();
      idx += 4;
      checkLength("resourceSize", resourceSize, messageLength);
      String resourceName = toNonEmptyString(resourceSize, byteBuf);
      idx += resourceSize;

      // Partition
      byteBuf.readerIndex(idx);
      int partitionSize = byteBuf.readInt();
      idx += 4;
      checkLength("partitionSize", partitionSize, messageLength);
      String partitionName = toNonEmptyString(partitionSize, byteBuf);
      idx += partitionSize;

      // State
      byteBuf.readerIndex(idx);
      int stateSize = byteBuf.readInt();
      idx += 4;
      checkLength("stateSize", stateSize, messageLength);
      String state = toNonEmptyString(stateSize, byteBuf);
      idx += stateSize;

      // Source instance
      byteBuf.readerIndex(idx);
      int srcInstanceSize = byteBuf.readInt();
      idx += 4;
      checkLength("srcInstanceSize", srcInstanceSize, messageLength);
      String srcInstance = toNonEmptyString(srcInstanceSize, byteBuf);
      idx += srcInstanceSize;

      // Destination instance
      byteBuf.readerIndex(idx);
      int dstInstanceSize = byteBuf.readInt();
      idx += 4;
      checkLength("dstInstanceSize", dstInstanceSize, messageLength);
      String dstInstance = toNonEmptyString(dstInstanceSize, byteBuf);
      idx += dstInstanceSize;

      // Position at message
      byteBuf.readerIndex(idx + 4);

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
      callback.onMessage(scope, messageId, byteBuf);

      // Stats
      statRxMsg.mark();
      statRxBytes.mark(messageLength);
    } finally {
      byteBuf.release();
    }

  }
}
