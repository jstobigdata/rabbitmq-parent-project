package com.rabbitmq.spring.rtemplate;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RabbitTemplateExample {

  @Autowired
  private RabbitTemplate rabbitTemplate;

  public void sendHello() {
    rabbitTemplate.convertAndSend("Hello from Jstobigdata.com");
  }
}
