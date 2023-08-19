package dev.agasen.kafkaproducer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaRunner implements CommandLineRunner {

  private static final String TOPIC = "wikimedia.recentchanges";

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final WikimediaStreamClient wikimediaStreamClient;

  @Override
  public void run(String... args) throws Exception {
    Flux<String> eventStream = wikimediaStreamClient.getRecentChanges();
    eventStream
        .take(5) // take only 5 sequence of data
        .subscribe(this::consumeSuccess, this::consumeError);
  }

  public void consumeSuccess(String s) {
    log.info("consuming... " + s);
    kafkaTemplate.send(TOPIC, s);
  }

  public void consumeError(Throwable t) {
    log.error("With Error... ", t);
  }
}
