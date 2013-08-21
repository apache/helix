package org.apache.helix.recipes.rabbitmq;

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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Emitter {

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err
          .println("USAGE: java Emitter rabbitmqServer (e.g. localhost) numberOfMessage (optional)");
      System.exit(1);
    }

    final String mqServer = args[0]; // "zzhang-ld";
    int count = Integer.MAX_VALUE;
    if (args.length > 1) {
      try {
        count = Integer.parseInt(args[1]);
      } catch (Exception e) {
        // TODO: handle exception
      }
    }
    System.out.println("Sending " + count + " messages with random topic id");

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(mqServer);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "topic");

    for (int i = 0; i < count; i++) {
      int rand = ((int) (Math.random() * 10000) % SetupConsumerCluster.DEFAULT_PARTITION_NUMBER);
      String routingKey = "topic_" + rand;
      String message = "message_" + rand;

      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
      System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

      Thread.sleep(1000);
    }

    connection.close();
  }

}
