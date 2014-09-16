package org.apache.helix.ipc.benchmark;

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
import io.netty.buffer.ByteBufAllocator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MXBean;
import javax.management.ObjectName;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.helix.ipc.HelixIPCCallback;
import org.apache.helix.ipc.HelixIPCService;
import org.apache.helix.ipc.netty.NettyHelixIPCService;
import org.apache.helix.resolver.HelixAddress;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.log4j.BasicConfigurator;

import com.google.common.collect.ImmutableSet;

/**
 * Run with following to enable JMX:
 * -Dcom.sun.management.jmxremote <br/>
 * -Dcom.sun.management.jmxremote.port=10000 <br/>
 * -Dcom.sun.management.jmxremote.authenticate=false <br/>
 * -Dcom.sun.management.jmxremote.ssl=false <br/>
 */
public class BenchmarkDriver implements Runnable {

  private static final int MESSAGE_TYPE = 1025;

  private final int port;
  private final int numPartitions;
  private final AtomicBoolean isShutdown;
  private final byte[] messageBytes;
  private final int numConnections;

  private HelixIPCService ipcService;
  private String localhost;
  private Thread[] trafficThreads;

  public BenchmarkDriver(int port, int numPartitions, int numThreads, int messageSize, int numConnections) {
    this.port = port;
    this.numPartitions = numPartitions;
    this.isShutdown = new AtomicBoolean(true);
    this.trafficThreads = new Thread[numThreads];
    this.numConnections = numConnections;

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < messageSize; i++) {
      sb.append("A");
    }
    this.messageBytes = sb.toString().getBytes();
  }

  @Override
  public void run() {
    try {
      // Register controller MBean
      final BenchmarkDriver driver = this;
      ManagementFactory.getPlatformMBeanServer().registerMBean(new Controller() {
        @Override
        public void startTraffic(String remoteHost, int remotePort) {
          driver.startTraffic(remoteHost, remotePort);
        }

        @Override
        public void stopTraffic() {
          driver.stopTraffic();
        }
      }, new ObjectName("org.apache.helix:type=BenchmarkDriver"));

      // The local server
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
      ipcService =
          new NettyHelixIPCService(new NettyHelixIPCService.Config()
              .setInstanceName(localhost + "_" + port).setPort(port)
              .setNumConnections(numConnections));

      // Counts number of messages received, and ack them
      ipcService.registerCallback(MESSAGE_TYPE, new HelixIPCCallback() {
        @Override
        public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
          // Do nothing
        }
      });

      ipcService.start();
      System.out.println("Started IPC service on "
          + InetAddress.getLocalHost().getCanonicalHostName() + ":" + port);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startTraffic(final String remoteHost, final int remotePort) {
    if (isShutdown.getAndSet(false)) {
      System.out.println("Starting " + trafficThreads.length + " threads to generate traffic");
      for (int i = 0; i < trafficThreads.length; i++) {
        Thread t = new Thread() {
          @Override
          public void run() {
            ByteBuf m = ByteBufAllocator.DEFAULT.buffer(messageBytes.length);
            m.writeBytes(messageBytes);
            while (!isShutdown.get()) {
              for (int i = 0; i < numPartitions; i++) {
                HelixMessageScope scope =
                    new HelixMessageScope.Builder().cluster("CLUSTER").resource("RESOURCE")
                        .partition("PARTITION_" + i).sourceInstance(localhost + "_" + port).build();

                Set<HelixAddress> destinations =
                    ImmutableSet.of(new HelixAddress(scope, remoteHost + "_" + remotePort,
                        new InetSocketAddress(remoteHost, remotePort)));

                UUID uuid = UUID.randomUUID();

                try {
                  for (HelixAddress destination : destinations) {
                    m.retain();
                    ipcService.send(destination, MESSAGE_TYPE, uuid, m);
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            }
          }
        };
        t.start();
        trafficThreads[i] = t;
      }
      System.out.println("Started traffic to " + remoteHost + ":" + remotePort);
    }
  }

  private void stopTraffic() {
    if (!isShutdown.getAndSet(true)) {
      try {
        for (Thread t : trafficThreads) {
          t.join();
        }
        System.out.println("Stopped traffic");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @MXBean
  public interface Controller {
    void startTraffic(String remoteHost, int remotePort);
    void stopTraffic();
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Options options = new Options();
    options.addOption("partitions", true, "Number of partitions");
    options.addOption("threads", true, "Number of threads");
    options.addOption("messageSize", true, "Message size in bytes");
    options.addOption("numConnections", true, "Number of connections between nodes");

    CommandLine commandLine = new GnuParser().parse(options, args);

    if (commandLine.getArgs().length != 1) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("usage: [options] port", options);
      System.exit(1);
    }

    final CountDownLatch latch = new CountDownLatch(1);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        latch.countDown();
      }
    });

    new BenchmarkDriver(Integer.parseInt(commandLine.getArgs()[0]), Integer.parseInt(commandLine
        .getOptionValue("partitions", "1")), Integer.parseInt(commandLine.getOptionValue("threads",
        "1")), Integer.parseInt(commandLine.getOptionValue("messageSize", "1024")),
        Integer.parseInt(commandLine.getOptionValue("numConnections", "1"))).run();

    latch.await();
  }
}
