package com.linkedin.helix.recipes.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Emitter
{

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void main(String[] argv) throws Exception
  {
    final String mqServer = "zzhang-ld";

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(mqServer);
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, "topic");

    for (int i = 0; i < 10; i++)
    {
      int rand = ((int) (Math.random() * 10000) % 60);
      String routingKey = "topic_" + rand;
      String message = "message_" + rand;

      channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
      System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
      
      Thread.sleep(1000);
    }
    
    connection.close();
  }

}
