package com.amqp.exchanges;

import com.amqp.basic.queue.CommonConfigs;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

//Run me - second
public class CreateQueues {
  public static void main(String[] args) throws IOException, TimeoutException {

    ConnectionFactory factory = new ConnectionFactory();
    Connection connection = factory.newConnection(CommonConfigs.AMQP_URL);
    Channel channel = connection.createChannel();

    //Create the Queues
    channel.queueDeclare("MobileQ", true, false, false, null);
    channel.queueDeclare("ACQ", true, false, false, null);
    channel.queueDeclare("LightQ", true, false, false, null);

    channel.close();
    connection.close();
  }
}
