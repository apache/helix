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

import java.io.IOException;

import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class ConsumerThread extends Thread {
  private static final String EXCHANGE_NAME = "topic_logs";
  private final PartitionId _partition;
  private final String _mqServer;
  private final ParticipantId _consumerId;

  public ConsumerThread(PartitionId partition, String mqServer, ParticipantId consumerId) {
    _partition = partition;
    _mqServer = mqServer;
    _consumerId = consumerId;
  }

  @Override
  public void run() {
    Connection connection = null;
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(_mqServer);
      connection = factory.newConnection();
      Channel channel = connection.createChannel();

      channel.exchangeDeclare(EXCHANGE_NAME, "topic");
      String queueName = channel.queueDeclare().getQueue();

      String bindingKey = _partition.toString();
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);

      System.out.println(" [*] " + _consumerId + " Waiting for messages on " + bindingKey
          + ". To exit press CTRL+C");

      QueueingConsumer consumer = new QueueingConsumer(channel);
      channel.basicConsume(queueName, true, consumer);

      while (true) {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        String message = new String(delivery.getBody());
        String routingKey = delivery.getEnvelope().getRoutingKey();

        System.out.println(" [x] " + _consumerId + " Received '" + routingKey + "':'" + message
            + "'");
      }
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _consumerId + " on " + _partition + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
}
