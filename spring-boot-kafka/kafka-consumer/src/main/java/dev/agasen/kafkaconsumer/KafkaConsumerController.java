package dev.agasen.kafkaconsumer;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@RestController
@RequiredArgsConstructor
@RequestMapping("/server-events")
public class KafkaConsumerController {
  
  private final KafkaReceiver<String, String> kafkaReceiver;

  @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> getEvents() {
    Flux<ReceiverRecord<String, String>> kafkaFlux = kafkaReceiver.receive();
    return kafkaFlux.checkpoint("Message being consumed")
        .log()
        .doOnNext(r -> r.receiverOffset().acknowledge())
        .map(ReceiverRecord::value)
        .map(r -> ServerSentEvent.builder(r).build())
        .checkpoint();
  }
}
