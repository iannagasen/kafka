package dev.agasen.wikimedia;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@SpringBootApplication
public class WikimediaApplication {

  @Autowired private KafkaProducerService kafkaProducerService;
  @Autowired private WebClient webClient;
  
  public static void main(String[] args) {
    SpringApplication.run(WikimediaApplication.class, args);
  }

  @Bean
  public CommandLineRunner runner() {
    return args -> {
      Flux<String> changeStream = webClient.get()
        .retrieve()
        .bodyToFlux(String.class);

      changeStream.take(Duration.ofMinutes(5));
      
      changeStream.subscribe(
          kafkaProducerService::send,
          error -> log.error("Error: ", error),
          () -> log.info("Completed")      
      );
    };
  }
}