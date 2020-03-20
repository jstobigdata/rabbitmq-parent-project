package com.amqp.exchanges.all;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TopicExchange {
  public void declareQueues() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create Topic Queues
    channel.queueDeclare("Health-News", true, false, false, null);
    channel.queueDeclare("Sports-News", true, false, false, null);
    channel.queueDeclare("Education-News", true, false, false, null);
    channel.close();
  }

  public void declareExchange() throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create Topic Exchange
    channel.exchangeDeclare("my-topic-exchange", BuiltinExchangeType.TOPIC, true);
    channel.close();
  }

  public void declareBindings () throws IOException, TimeoutException {
    Channel channel = ConnectionManager.getConnection().createChannel();
    //Create bindings - (queue, exchange, routingKey) - routingKey != null
    channel.queueBind("Health-News", "my-topic-exchange", "*health*");
    channel.queueBind("Sports-News", "my-topic-exchange", "sports.*");
    channel.queueBind("Education-News", "my-topic-exchange", "education*");
    channel.close();
  }

  //TODO - subscribe
  //TODO - publish
  //TODO - psvm
}
