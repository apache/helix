package com.linkedin.helix.recipes.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class ConsumerThread extends Thread
{
  private static final String EXCHANGE_NAME = "topic_logs";
  private final String _partition;
  private final String _mqServer;
  private final String _consumerId;
  
  public ConsumerThread(String partition, String mqServer, String consumerId)
  {
    _partition = partition;
    _mqServer = mqServer;
    _consumerId = consumerId;
  }

  @Override
  public void run()
  {
    Connection connection = null;
    try
    {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(_mqServer);
      connection = factory.newConnection();
      Channel channel = connection.createChannel();

      channel.exchangeDeclare(EXCHANGE_NAME, "topic");
      String queueName = channel.queueDeclare().getQueue();

      String bindingKey = _partition;
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);

      System.out.println(" [*] " + _consumerId + " Waiting for messages on " + bindingKey + ". To exit press CTRL+C");

      QueueingConsumer consumer = new QueueingConsumer(channel);
      channel.basicConsume(queueName, true, consumer);

      while (true)
      {
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        String message = new String(delivery.getBody());
        String routingKey = delivery.getEnvelope().getRoutingKey();

        System.out.println(" [x] " + _consumerId + " Received '" + routingKey + "':'" + message + "'");
      }
    } catch (InterruptedException e)
    {
      System.err.println(" [-] " + _consumerId + " on " + _partition + " is interrupted ...");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    } finally
    {
      if (connection != null)
      {
        try
        {
          connection.close();
        } catch (IOException e)
        {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
}
