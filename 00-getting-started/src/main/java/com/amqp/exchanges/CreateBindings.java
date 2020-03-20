package com.amqp.exchanges;

import com.amqp.basic.queue.CommonConfigs;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

//Run me - Third
public class CreateBindings {
  public static void main(String[] args) throws IOException, TimeoutException {

    ConnectionFactory connectionFactory = new ConnectionFactory();
    Connection connection = connectionFactory.newConnection(CommonConfigs.AMQP_URL);
    try (Channel channel = connection.createChannel()) {
      //Create bindings - (queue, exchange, routingKey)
      channel.queueBind("MobileQ", "my-direct-exchange", "personalDevice");
      channel.queueBind("ACQ", "my-direct-exchange", "homeAppliance");
      channel.queueBind("LightQ", "my-direct-exchange", "homeAppliance");
    }
    connection.close();
  }
}
