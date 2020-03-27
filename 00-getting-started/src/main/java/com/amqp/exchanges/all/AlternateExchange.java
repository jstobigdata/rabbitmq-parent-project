package com.amqp.exchanges.all;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class AlternateExchange {

  /**
   * Declare exchanges - "alt.fanout.exchange", "alternate-exchange"
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareExchange() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Declare the fanout exchange
    channel.exchangeDeclare("alt.fanout.exchange", BuiltinExchangeType.FANOUT, true);

    //Declare the topic exchange and set the alternate-exchange = "alt.fanout.exchange"
    Map<String, Object> arguments = new HashMap<>();
    arguments.put("alternate-exchange", "alt.fanout.exchange");
    channel.exchangeDeclare("alt.topic.exchange", BuiltinExchangeType.TOPIC, true, false, arguments);
    channel.close();
  }

  public static void declareQueues() throws IOException, TimeoutException {
    //Create a channel - do not share the Channel instance
    Channel channel = ConnectionManager.getConnection().createChannel();

    //Create the Queues for "alt.topic.exchange".
    channel.queueDeclare("HealthQ", true, false, false, null);
    channel.queueDeclare("SportsQ", true, false, false, null);
    channel.queueDeclare("EducationQ", true, false, false, null);

    //Create the Queues for "alt.fanout.exchange"
    channel.queueDeclare("altQueue", true, false, false, null);

    //Close the channel
    channel.close();
  }

  /**
   * Declare Queue bindings.
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareBindings() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create bindings for alt.topic.exchange - (queue, exchange, routingKey)
    channel.queueBind("HealthQ", "alt.topic.exchange", "health.*");
    channel.queueBind("SportsQ", "alt.topic.exchange", "#.sports.*");
    channel.queueBind("EducationQ", "alt.topic.exchange", "#.education");

    //Binding for fanoutExchange - alt.fanout.exchange and altQueue
    channel.queueBind("altQueue", "alt.fanout.exchange", "");

    channel.close();
  }


  /**
   * Subscribe the Queues.
   * @throws IOException
   * @throws TimeoutException
   */
  public static void subscribeMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();

    DeliverCallback deliverCallback = (consumerTag, message) -> {
      System.out.println("\n" + consumerTag);
      System.out.println(new String(message.getBody()));
    };

    CancelCallback cancelCallback = consumerTag -> {
      System.out.println(consumerTag);
    };

    channel.basicConsume("HealthQ", true, deliverCallback, cancelCallback);
    channel.basicConsume("SportsQ", true, deliverCallback, cancelCallback);
    channel.basicConsume("EducationQ", true, deliverCallback, cancelCallback);

    //Consume the rejected message
    channel.basicConsume("altQueue", true, (consumerTag, message) -> {
      System.out.println("\n" + consumerTag);
      System.out.println("rejected message: " + new String(message.getBody()));
    }, consumerTag -> {
    });
  }

  /**
   * Publish a valid and a rejected message.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void publishMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    String message = "Learn something new everyday";
    channel.basicPublish("alt.topic.exchange", "education", null, message.getBytes());

    //No Queue for this routingKey.
    message = "Stay fit in Mind and Body";
    channel.basicPublish("alt.topic.exchange", "education.health", null, message.getBytes());

    channel.close();
  }

  /**
   * Execute the methods.
   *
   * @param args
   * @throws IOException
   * @throws TimeoutException
   */
  public static void main(String[] args) throws IOException, TimeoutException {
    AlternateExchange.declareQueues();
    AlternateExchange.declareExchange();
    AlternateExchange.declareBindings();

    //Threads created to publish-subscribe asynchronously
    Thread subscribe = new Thread() {
      @Override
      public void run() {
        try {
          AlternateExchange.subscribeMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    Thread publish = new Thread() {
      @Override
      public void run() {
        try {
          AlternateExchange.publishMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    subscribe.start();
    publish.start();
  }
}
