package com.amqp.exchanges.all;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TopicExchange {
  /**
   * Declare a Topic Exchange with the name my-topic-exchange.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareExchange() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create Topic Exchange
    channel.exchangeDeclare("my-topic-exchange", BuiltinExchangeType.TOPIC, true);
    channel.close();
  }

  /**
   * Declare Queues to receive respective interested messages.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareQueues() throws IOException, TimeoutException {
    //Create a channel - do not share the Channel instance
    Channel channel = ConnectionManager.getConnection().createChannel();

    //Create the Queues
    channel.queueDeclare("HealthQ", true, false, false, null);
    channel.queueDeclare("SportsQ", true, false, false, null);
    channel.queueDeclare("EducationQ", true, false, false, null);

    channel.close();
  }

  /**
   * Declare Bindings - register interests using routing key patterns.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareBindings() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create bindings - (queue, exchange, routingKey) - routingKey != null
    channel.queueBind("HealthQ", "my-topic-exchange", "health.*");
    channel.queueBind("SportsQ", "my-topic-exchange", "#.sports.*");
    channel.queueBind("EducationQ", "my-topic-exchange", "#.education");
    channel.close();
  }

  /**
   * Assign Consumers to each of the Queue.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void subscribeMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    channel.basicConsume("HealthQ", true, ((consumerTag, message) -> {
      System.out.println("\n\n=========== Health Queue ==========");
      System.out.println(consumerTag);
      System.out.println("HealthQ: " + new String(message.getBody()));
      System.out.println(message.getEnvelope());
    }), consumerTag -> {
      System.out.println(consumerTag);
    });

    channel.basicConsume("SportsQ", true, ((consumerTag, message) -> {
      System.out.println("\n\n ============ Sports Queue ==========");
      System.out.println(consumerTag);
      System.out.println("SportsQ: " + new String(message.getBody()));
      System.out.println(message.getEnvelope());
    }), consumerTag -> {
      System.out.println(consumerTag);
    });

    channel.basicConsume("EducationQ", true, ((consumerTag, message) -> {
      System.out.println("\n\n ============ Education Queue ==========");
      System.out.println(consumerTag);
      System.out.println("EducationQ: " + new String(message.getBody()));
      System.out.println(message.getEnvelope());
    }), consumerTag -> {
      System.out.println(consumerTag);
    });
  }

  /**
   * Publish Messages with different routing keys.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void publishMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    String message = "Drink a lot of Water and stay Healthy!";
    //channel.basicPublish("my-topic-exchange", "sports.sports.sports", null, message.getBytes());
    channel.basicPublish("my-topic-exchange", "health.education", null, message.getBytes());

    message = "Learn something new everyday";
    channel.basicPublish("my-topic-exchange", "education", null, message.getBytes());

    message = "Stay fit in Mind and Body";
    channel.basicPublish("my-topic-exchange", "education.health", null, message.getBytes());

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
    TopicExchange.declareExchange();
    TopicExchange.declareQueues();
    TopicExchange.declareBindings();

    Thread subscribe = new Thread(() -> {
      try {
        TopicExchange.subscribeMessage();
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    });

    Thread publish = new Thread(() -> {
      try {
        TopicExchange.publishMessage();
      } catch (IOException | TimeoutException e) {
        e.printStackTrace();
      }
    });
    subscribe.start();
    publish.start();
  }
}

