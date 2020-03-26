package com.amqp.exchanges.all;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ExchangeToExchange {

  //Step-1: Declare the exchange
  public static void declareExchanges() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Declare both the exchanges - linked-direct-exchange and home-direct-exchange.
    channel.exchangeDeclare("linked-direct-exchange", BuiltinExchangeType.DIRECT, true);
    channel.exchangeDeclare("home-direct-exchange", BuiltinExchangeType.DIRECT, true);
    channel.close();
  }

  //Step-2: Declare the Queues
  public static void declareQueues() throws IOException, TimeoutException {
    //Create a channel - do not share the Channel instance
    Channel channel = ConnectionManager.getConnection().createChannel();

    //Create the Queues for linked-direct-exchange
    channel.queueDeclare("MobileQ", true, false, false, null);
    channel.queueDeclare("ACQ", true, false, false, null);
    channel.queueDeclare("LightQ", true, false, false, null);

    //Create the Queues for home-direct-exchange
    channel.queueDeclare("FanQ", true, false, false, null);
    channel.queueDeclare("LaptopQ", true, false, false, null);
    channel.close();
  }

  //Step-3: Create the Bindings  between respective Queues and Exchanges
  public static void declareQueueBindings() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create bindings - (queue, exchange, routingKey)
    channel.queueBind("MobileQ", "linked-direct-exchange", "personalDevice");
    channel.queueBind("ACQ", "linked-direct-exchange", "homeAppliance");
    channel.queueBind("LightQ", "linked-direct-exchange", "homeAppliance");

    //Create the bindings - with home-direct-exchange
    channel.queueBind("FanQ", "home-direct-exchange", "homeAppliance");
    channel.queueBind("LaptopQ", "home-direct-exchange", "personalDevice");
    channel.close();
  }

  //Step-4: Create the Bindings between Exchanges.
  public static void declareExchangesBindings() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    channel.exchangeBind("linked-direct-exchange", "home-direct-exchange", "homeAppliance");
    channel.close();
  }

  //Step-5: Create the Subscribers
  public static void subscribeMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    channel.basicConsume("MobileQ", true, (consumerTag, message) -> {
      System.out.println("\n\n" + message.getEnvelope());
      System.out.println("MobileQ:" + new String(message.getBody()));
    }, consumerTag -> {
      System.out.println(consumerTag);
    });

    channel.basicConsume("ACQ", true, (consumerTag, message) -> {
      System.out.println("\n\n" + message.getEnvelope());
      System.out.println("ACQ:" + new String(message.getBody()));
    }, consumerTag -> {
      System.out.println(consumerTag);
    });
    channel.basicConsume("LightQ", true, (consumerTag, message) -> {
      System.out.println("\n\n" + message.getEnvelope());
      System.out.println("LightQ:" + new String(message.getBody()));
    }, consumerTag -> {
      System.out.println(consumerTag);
    });

    channel.basicConsume("LaptopQ", true, (consumerTag, message) -> {
      System.out.println("\n\n" + message.getEnvelope());
      System.out.println("LaptopQ:" + new String(message.getBody()));
    }, consumerTag -> {
      System.out.println(consumerTag);
    });
    channel.basicConsume("FanQ", true, (consumerTag, message) -> {
      System.out.println("\n\n" + message.getEnvelope());
      System.out.println("FanQ:" + new String(message.getBody()));
    }, consumerTag -> {
      System.out.println(consumerTag);
    });
  }

  //Step-6: Publish a message to home-direct-exchange.
  public static void publishMessage() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    String message = "Direct message - Turn on the Home Appliances ";
    channel.basicPublish("home-direct-exchange", "homeAppliance", null, message.getBytes());
    channel.close();
  }

  public static void main(String[] args) throws IOException, TimeoutException {
    ExchangeToExchange.declareExchanges();
    ExchangeToExchange.declareQueues();
    ExchangeToExchange.declareQueueBindings();
    ExchangeToExchange.declareExchangesBindings();

    //Threads created to publish-subscribe asynchronously
    Thread subscribe = new Thread() {
      @Override
      public void run() {
        try {
          ExchangeToExchange.subscribeMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    Thread publish = new Thread() {
      @Override
      public void run() {
        try {
          ExchangeToExchange.publishMessage();
        } catch (IOException | TimeoutException e) {
          e.printStackTrace();
        }
      }
    };

    subscribe.start();
    publish.start();
  }
}
