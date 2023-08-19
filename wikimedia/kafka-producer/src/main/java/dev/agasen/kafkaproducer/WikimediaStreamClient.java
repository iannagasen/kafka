package dev.agasen.kafkaproducer;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

@Component
@RequiredArgsConstructor
public class WikimediaStreamClient {
  
  private final WebClient webClient;

  private static final String URL = "stream.wikimedia.org";

  public Flux<String> getRecentChanges() {
    return webClient.get()
        .uri(URL + "/v2/stream/recentchange")
        .retrieve()
        .bodyToFlux(String.class);
  }

}
