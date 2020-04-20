package com.zt.kafka.producter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * kafka消息队列生产者
 */
@Component
public class TestProducter {
    @Autowired
    private KafkaTemplate<String,Object> kafkaTemplate;

    public void sendMsg(String msg) {
        kafkaTemplate.send("test", msg);
    }

}
