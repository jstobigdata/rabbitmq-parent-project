package com.amqp.exchanges;

import com.amqp.basic.queue.CommonConfigs;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TestPublisher {
  public static void main(String[] args) throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    Connection connection = factory.newConnection(CommonConfigs.AMQP_URL);
    Channel channel = connection.createChannel();

    //Basic publish
    String message = "Turn on home appliances";
    channel.basicPublish("my-direct-exchange", "homeAppliance", null, message.getBytes());

    //Close the channel and connection
    channel.close();
    connection.close();
  }
}