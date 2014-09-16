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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
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
import org.apache.helix.resolver.HelixMessageScope;
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
  private static final int MAX_FRAME_LENGTH = 1024 * 1024;
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

      new ServerBootstrap().group(eventLoopGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_KEEPALIVE, true).childOption(ChannelOption.SO_KEEPALIVE, true)
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel socketChannel) throws Exception {
              socketChannel.pipeline().addLast(
                  new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, LENGTH_FIELD_OFFSET,
                      LENGTH_FIELD_LENGTH, LENGTH_ADJUSTMENT, INITIAL_BYTES_TO_STRIP));
              socketChannel.pipeline().addLast(new HelixIPCCallbackHandler());
            }
          }).bind(new InetSocketAddress(config.getPort()));

      clientBootstrap =
          new Bootstrap().group(eventLoopGroup).channel(NioSocketChannel.class)
              .option(ChannelOption.SO_KEEPALIVE, true)
              .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                  socketChannel.pipeline().addLast(
                      new LengthFieldPrepender(LENGTH_FIELD_LENGTH, true));
                  socketChannel.pipeline().addLast(new BackPressureHandler());
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
      eventLoopGroup.shutdownGracefully();
    }
  }

  /**
   * Sends a message to all partitions with a given state in the cluster.
   */
  @Override
  public void send(HelixAddress destination, int messageType, UUID messageId, ByteBuf message) {
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
            if (channel != null && channel.isOpen()) {
              channel.close();
            }
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
      BackPressureHandler backPressureHandler = channel.pipeline().get(BackPressureHandler.class);
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
  }

  private class BackPressureHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final Object sync = new Object();

    BackPressureHandler() {
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

  @ChannelHandler.Sharable
  private class HelixIPCCallbackHandler extends SimpleChannelInboundHandler<ByteBuf> {

    HelixIPCCallbackHandler() {
      super(false); // we will manage reference
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
        } else if (!dstInstance.equals(config.getInstanceName())) {
          throw new IllegalStateException(config.getInstanceName()
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

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable cause) {
      LOG.error(cause);
    }
  }

  /** Given a byte buf w/ a certain reader index, encodes the next length bytes as a String */
  private static String toNonEmptyString(int length, ByteBuf byteBuf) {
    if (byteBuf.readableBytes() >= length) {
      return byteBuf.toString(byteBuf.readerIndex(), length, Charset.defaultCharset());
    }
    return null;
  }

  /** Writes [s.length(), s] to buf, or [0] if s is null */
  private static void writeStringWithLength(ByteBuf buf, String s) {
    if (s == null) {
      buf.writeInt(0);
      return;
    }

    buf.writeInt(s.length());
    for (int i = 0; i < s.length(); i++) {
      buf.writeByte(s.charAt(i));
    }
  }

  /** Returns the length of a string, or 0 if s is null */
  private static int getLength(String s) {
    return s == null ? 0 : s.length();
  }

  /**
   * @throws java.lang.IllegalArgumentException if length > messageLength (attempt to prevent OOM
   *           exceptions)
   */
  private static void checkLength(String fieldName, int length, int messageLength)
      throws IllegalArgumentException {
    if (length > messageLength) {
      throw new IllegalArgumentException(fieldName + "=" + length
          + " is greater than messageLength=" + messageLength);
    }
  }

  public static class Config {
    private String instanceName;
    private int port;
    private int numConnections = 1;
    private long maxChannelLifeMillis = 5000;

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

    public String getInstanceName() {
      return instanceName;
    }

    public int getPort() {
      return port;
    }

    public int getNumConnections() {
      return numConnections;
    }
  }
}
