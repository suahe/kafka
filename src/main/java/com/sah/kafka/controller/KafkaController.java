package com.sah.kafka.controller;

import com.sah.kafka.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author suahe
 * @date 2022/4/9
 * @ApiNote
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @GetMapping("/sendMsg")
    public String sendMsg(){
        kafkaProducer.send("this is a test kafka topic message！");
        return "send success！";
    }
}
