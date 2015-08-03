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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.ipc.HelixIPCCallback;
import org.apache.helix.ipc.HelixIPCService;
import org.apache.helix.resolver.HelixAddress;
import org.apache.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * Provides partition/state-level messaging among nodes in a Helix cluster.
 * <p>
 * The message format is (where len == 4B, and contains the length of the next field)
 *
 * <pre>
 *      +----------------------+
 *      | totalLength (4B)     |
 *      +----------------------+
 *      | version (4B)         |
 *      +----------------------+
 *      | messageType (4B)     |
 *      +----------------------+
 *      | messageId (16B)      |
 *      +----------------------+
 *      | len | cluster        |
 *      +----------------------+
 *      | len | resource       |
 *      +----------------------+
 *      | len | partition      |
 *      +----------------------+
 *      | len | state          |
 *      +----------------------+
 *      | len | srcInstance    |
 *      +----------------------+
 *      | len | dstInstance    |
 *      +----------------------+
 *      | len | message        |
 *      +----------------------+
 * </pre>
 *
 * </p>
 */
public class NettyHelixIPCService implements HelixIPCService {

  private static final Logger LOG = Logger.getLogger(NettyHelixIPCService.class);
  private static final int MESSAGE_VERSION = 1;

  // Parameters for length header field of message (tells decoder to interpret but preserve length
  // field in message)
  private static final int LENGTH_FIELD_OFFSET = 0;
  private static final int LENGTH_FIELD_LENGTH = 4;
  private static final int LENGTH_ADJUSTMENT = -4;
  private static final int INITIAL_BYTES_TO_STRIP = 0;
  private static final int NUM_LENGTH_FIELDS = 7;

  private final Config config;
  private final AtomicBoolean isShutdown;
  private final Map<InetSocketAddress, List<Channel>> channelMap;
  private final MetricRegistry metricRegistry;
  private final ConcurrentMap<Integer, HelixIPCCallback> callbacks;

  private EventLoopGroup eventLoopGroup;
  private Bootstrap clientBootstrap;
  private Meter statTxMsg;
  private Meter statRxMsg;
  private Meter statTxBytes;
  private Meter statRxBytes;
  private Counter statChannelOpen;
  private Counter statError;
  private JmxReporter jmxReporter;

  public NettyHelixIPCService(Config config) {
    super();
    this.config = config;
    this.isShutdown = new AtomicBoolean(true);
    this.channelMap = new HashMap<InetSocketAddress, List<Channel>>();
    this.metricRegistry = new MetricRegistry();
    this.callbacks = new ConcurrentHashMap<Integer, HelixIPCCallback>();
  }

  /**
   * Starts message handling server, creates client bootstrap, and bootstraps partition routing
   * table.
   */
  public void start() throws Exception {
    if (isShutdown.getAndSet(false)) {
      eventLoopGroup = new NioEventLoopGroup();

      statTxMsg = metricRegistry.meter(MetricRegistry.name(NettyHelixIPCService.class, "txMsg"));
      statRxMsg = metricRegistry.meter(MetricRegistry.name(NettyHelixIPCService.class, "rxMsg"));
      statTxBytes =
          metricRegistry.meter(MetricRegistry.name(NettyHelixIPCService.class, "txBytes"));
      statRxBytes =
          metricRegistry.meter(MetricRegistry.name(NettyHelixIPCService.class, "rxBytes"));
      statChannelOpen =
          metricRegistry.counter(MetricRegistry.name(NettyHelixIPCService.class, "channelOpen"));
      statError = metricRegistry.counter(MetricRegistry.name(NettyHelixIPCService.class, "error"));

      // Report metrics via JMX
      jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
      jmxReporter.start();
      LOG.info("Registered JMX metrics reporter");

      new ServerBootstrap().group(eventLoopGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true).childOption(ChannelOption.SO_KEEPALIVE, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
              socketChannel.pipeline().addLast(
                  new LengthFieldBasedFrameDecoder(
                      config.getMaxFrameLength(),
                      LENGTH_FIELD_OFFSET,
                      LENGTH_FIELD_LENGTH,
                      LENGTH_ADJUSTMENT,
                      INITIAL_BYTES_TO_STRIP));
              socketChannel.pipeline().addLast(
                  new NettyHelixIPCCallbackHandler(
                      config.getInstanceName(),
                      callbacks,
                      statRxMsg,
                      statRxBytes));
            }
          }).bind(new InetSocketAddress(config.getPort()));
      LOG.info("Listening on port " + config.getPort());

      clientBootstrap =
          new Bootstrap().group(eventLoopGroup).channel(NioSocketChannel.class)
              .option(ChannelOption.SO_KEEPALIVE, true)
              .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                  socketChannel.pipeline().addLast(new LengthFieldPrepender(LENGTH_FIELD_LENGTH, true));
                  socketChannel.pipeline().addLast(new NettyHelixIPCBackPressureHandler());
                }
              });
    }
  }

  /**
   * Shuts down event loops for message handling server and message passing client.
   */
  public void shutdown() throws Exception {
    if (!isShutdown.getAndSet(true)) {
      jmxReporter.stop();
      LOG.info("Stopped JMX reporter");

      eventLoopGroup.shutdownGracefully();
      LOG.info("Shut down event loop group");
    }
  }

  /**
   * Sends a message to all partitions with a given state in the cluster.
   */
  @Override
  public void send(HelixAddress destination, int messageType, UUID messageId, ByteBuf message) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Sending " + messageId);
    }
    // Send message
    try {
      // Get list of channels
      List<Channel> channels = channelMap.get(destination.getSocketAddress());
      if (channels == null) {
        synchronized (channelMap) {
          channels = channelMap.get(destination.getSocketAddress());
          if (channels == null) {
            channels = new ArrayList<Channel>(config.getNumConnections());
            for (int i = 0; i < config.getNumConnections(); i++) {
              channels.add(null);
            }
            channelMap.put(destination.getSocketAddress(), channels);
          }
        }
      }

      // Pick the channel for this scope
      int idx = (Integer.MAX_VALUE & destination.getScope().hashCode()) % channels.size();
      Channel channel = channels.get(idx);
      if (channel == null || !channel.isOpen()) {
        synchronized (channelMap) {
          channel = channels.get(idx);
          if (channel == null || !channel.isOpen()) {
            channel = clientBootstrap.connect(destination.getSocketAddress()).sync().channel();
            channels.set(idx, channel);
            statChannelOpen.inc();
          }
        }
      }

      // Compute total length
      int headerLength =
          NUM_LENGTH_FIELDS
              * (Integer.SIZE / 8)
              + (Integer.SIZE / 8)
              * 2 // version, type
              + (Long.SIZE / 8)
              * 2 // 128 bit UUID
              + getLength(destination.getScope().getCluster())
              + getLength(destination.getScope().getResource())
              + getLength(destination.getScope().getPartition())
              + getLength(destination.getScope().getState()) + getLength(config.getInstanceName())
              + getLength(destination.getInstanceName());
      int messageLength = message == null ? 0 : message.readableBytes();

      // Build message header
      ByteBuf headerBuf = channel.alloc().buffer(headerLength);
      headerBuf.writeInt(MESSAGE_VERSION).writeInt(messageType)
          .writeLong(messageId.getMostSignificantBits())
          .writeLong(messageId.getLeastSignificantBits());
      writeStringWithLength(headerBuf, destination.getScope().getCluster());
      writeStringWithLength(headerBuf, destination.getScope().getResource());
      writeStringWithLength(headerBuf, destination.getScope().getPartition());
      writeStringWithLength(headerBuf, destination.getScope().getState());
      writeStringWithLength(headerBuf, config.getInstanceName());
      writeStringWithLength(headerBuf, destination.getInstanceName());

      // Compose message header and payload
      headerBuf.writeInt(messageLength);
      CompositeByteBuf fullByteBuf = channel.alloc().compositeBuffer(2);
      fullByteBuf.addComponent(headerBuf);
      fullByteBuf.writerIndex(headerBuf.readableBytes());
      if (message != null) {
        fullByteBuf.addComponent(message);
        fullByteBuf.writerIndex(fullByteBuf.writerIndex() + message.readableBytes());
      }

      // Send
      NettyHelixIPCBackPressureHandler backPressureHandler
          = channel.pipeline().get(NettyHelixIPCBackPressureHandler.class);
      backPressureHandler.waitUntilWritable(channel);
      channel.writeAndFlush(fullByteBuf);

      statTxMsg.mark();
      statTxBytes.mark(fullByteBuf.readableBytes());
    } catch (Exception e) {
      statError.inc();
      throw new IllegalStateException("Could not send message to " + destination, e);
    }
  }

  @Override
  public void registerCallback(int messageType, HelixIPCCallback callback) {
    callbacks.put(messageType, callback);
    LOG.info("Registered callback " + callback + " for message type " + messageType);
  }

  public static class Config {
    private String instanceName;
    private int port;
    private int numConnections = 1;
    private int maxFrameLength = 128 * 1024 * 1024;

    public Config setInstanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Config setPort(int port) {
      this.port = port;
      return this;
    }

    public Config setNumConnections(int numConnections) {
      this.numConnections = numConnections;
      return this;
    }

    public Config setMaxFrameLength(int maxFrameLength) {
      this.maxFrameLength = maxFrameLength;
      return this;
    }

    public String getInstanceName() {
      return instanceName;
    }

    public int getPort() {
      return port;
    }

    public int getNumConnections() {
      return numConnections;
    }

    public int getMaxFrameLength() {
      return maxFrameLength;
    }
  }
}
