package com.amqp.exchanges;

import com.amqp.basic.queue.CommonConfigs;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

//Run me first
public class CreateExchange {
  public static void main(String[] args) throws IOException, TimeoutException {

    ConnectionFactory factory = new ConnectionFactory();
    Connection connection = factory.newConnection(CommonConfigs.AMQP_URL);
    Channel channel = connection.createChannel();

    //Create an exchange
    channel.exchangeDeclare("my-direct-exchange", BuiltinExchangeType.DIRECT, true);
    //TODO - Uncomment the below lines and see
    //channel.exchangeDeclare("my-direct-exchange", BuiltinExchangeType.DIRECT, true);
    //channel.exchangeDeclare("my-direct-exchange", BuiltinExchangeType.DIRECT, true);
    channel.close();
    connection.close();
  }
}
