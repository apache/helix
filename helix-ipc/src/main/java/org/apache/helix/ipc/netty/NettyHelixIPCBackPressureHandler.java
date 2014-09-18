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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.log4j.Logger;

public class NettyHelixIPCBackPressureHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger LOG = Logger.getLogger(NettyHelixIPCBackPressureHandler.class);

  private final Object sync = new Object();

  public NettyHelixIPCBackPressureHandler() {
    super(false);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    ctx.fireChannelRead(msg);
  }

  public void waitUntilWritable(Channel channel) throws InterruptedException {
    synchronized (sync) {
      while (channel.isOpen() && !channel.isWritable()) {
        LOG.warn(channel + " is not writable, waiting until it is");
        sync.wait();
      }
    }
  }

  @Override
  public void channelWritabilityChanged(ChannelHandlerContext ctx) {
    synchronized (sync) {
      if (ctx.channel().isWritable()) {
        sync.notifyAll();
      }
    }
  }
}
