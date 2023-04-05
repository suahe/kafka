package com.sah.kafka.consumer;

import com.sah.kafka.common.KafkaConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * @author suahe
 * @date 2022/4/9
 * @ApiNote kafka消费者
 */
@Slf4j
@Component
public class KafkaConsumer {

    @KafkaListener(topics = KafkaConstant.TOPIC_TEST1)
    public void topicTest1(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        Optional message = Optional.ofNullable(record.value());
        if (message.isPresent()) {
            Object msg = message.get();
            log.info("topic_test1 消费了： Topic:" + topic + ",Message:" + msg);
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = {KafkaConstant.TOPIC_TEST1, KafkaConstant.TOPIC_TEST2})
    public void topicTest2(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {

        Optional message = Optional.ofNullable(record.value());
        if (message.isPresent()) {
            Object msg = message.get();
            log.info("topic_test2 消费了： Topic:" + topic + ",Message:" + msg);
            ack.acknowledge();
        }
    }
}
