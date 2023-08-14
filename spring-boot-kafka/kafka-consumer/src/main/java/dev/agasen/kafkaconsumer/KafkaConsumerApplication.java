package dev.agasen.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
public class KafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

	// @KafkaListener(id = "myGroup", topics = "topic_ni_ian")
	// public void listen(String data) {
	// 	log.info("\nREADING DATA -- " + data);
	// }

}
