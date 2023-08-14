package dev.agasen.wikimedia;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class WikimediaApplication {

  private final KafkaReceiver<String, String> kafkaReceiver;
  private final WebClient webClient;
  
  public static void main(String[] args) {
    SpringApplication.run(WikimediaApplication.class, args);
  }

  @Bean
  public CommandLineRunner runner() {
    return args -> {
      Flux<String> changeStream = webClient.get()
        .retrieve()
        .bodyToFlux(String.class);

      changeStream.subscribe(
        change -> log.info(change),
        error -> log.error("Error: ", error),
        () -> log.info("Completed")
      );
    };
  }
}