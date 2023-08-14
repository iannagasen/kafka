package dev.agasen.wikimedia;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaProducerService {
  
  private final KafkaTemplate<String, String> kafkaTemplate;

  public void send(String data) {
    log.info(data);
    kafkaTemplate.send("topic_ni_ian", data);
  }
}
