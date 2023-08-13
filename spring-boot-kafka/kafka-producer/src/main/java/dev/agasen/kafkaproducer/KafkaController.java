package dev.agasen.kafkaproducer;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/kafka")
public class KafkaController {
  
  private final KafkaProducer kafkaProducer;

  @PostMapping("/{data}")
  public void sendData(@PathVariable String data) {
    kafkaProducer.send(data);
  }
}
