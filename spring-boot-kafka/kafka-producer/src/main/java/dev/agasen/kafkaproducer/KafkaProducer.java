package dev.agasen.kafkaproducer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  public void send() {
    log.info("SENDING DATA");
    kafkaTemplate.send("topic1", "HELLO WORLD");
  }

  public void send(String data) {
    log.info("\nSENDING DATA : {}", data);
    kafkaTemplate.send("topic_ni_ian", data);
  }

}
