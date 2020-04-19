package com.amqp.exchanges.all;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HeadersExchange {

  /**
   * Declare a Headers Exchange.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareExchange() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Declare my-header-exchange
    channel.exchangeDeclare("my-header-exchange", BuiltinExchangeType.HEADERS, true);
    channel.close();
  }

  /**
   * Declare 3 Queues to demonstrate the example.
   *
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
   * Set the Bindings between Exchange and Queues.
   *
   * @throws IOException
   * @throws TimeoutException
   */
  public static void declareBindings() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create bindings - (queue, exchange, routingKey, headers) - routingKey != null
    Map<String, Object> healthArgs = new HashMap<>();
    healthArgs.put("x-match", "any"); //Match any of the header
    healthArgs.put("h1", "Header1");
    healthArgs.put("h2", "Header2");
    channel.queueBind("HealthQ", "my-header-exchange", "", healthArgs);

    Map<String, Object> sportsArgs = new HashMap<>();
    sportsArgs.put("x-match", "all"); //Match all of the header
    sportsArgs.put("h1", "Header1");
    sportsArgs.put("h2", "Header2");
    channel.queueBind("SportsQ", "my-header-exchange", "", sportsArgs);

    Map<String, Object> educationArgs = new HashMap<>();
    educationArgs.put("x-match", "any"); //Match any of the header
    educationArgs.put("h1", "Header1");
    educationArgs.put("h2", "Header2");
    channel.queueBind("EducationQ", "my-header-exchange", "", educationArgs);

    channel.close();
  }

  /**
   * Subscribe the Queues.
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

  public static void publishMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();

    String message = "Header Exchange example 1";
    Map<String, Object> headerMap = new HashMap<>();
    headerMap.put("h1", "Header1");
    headerMap.put("h3", "Header3");
    BasicProperties properties = new BasicProperties()
        .builder().headers(headerMap).build();
    channel.basicPublish("my-header-exchange", "", properties, message.getBytes());

    message = "Header Exchange example 2";
    headerMap.put("h2", "Header2");
    properties = new BasicProperties()
        .builder().headers(headerMap).build();
    channel.basicPublish("my-header-exchange", "", properties, message.getBytes());
    channel.close();
  }

  public static void main(String[] args) throws IOException, TimeoutException {
    HeadersExchange.declareQueues();
    HeadersExchange.declareExchange();
    HeadersExchange.declareBindings();

    //Threads created to publish-subscribe asynchronously
    Thread subscribe = new Thread() {
      @Override
      public void run() {
        try {
          HeadersExchange.subscribeMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    Thread publish = new Thread() {
      @Override
      public void run() {
        try {
          HeadersExchange.publishMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    subscribe.start();
    publish.start();
  }
}