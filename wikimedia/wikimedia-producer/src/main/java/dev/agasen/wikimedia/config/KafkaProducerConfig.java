package dev.agasen.wikimedia.config;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

@Configuration
public class KafkaProducerConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServer;

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> props = Map.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
    );

    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  // Reactive Kafka
  @Bean
  public KafkaReceiver<String, String> kafkaReceiver() {
    Map<String, Object> props = Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
        ConsumerConfig.GROUP_ID_CONFIG, "myGroup",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
    );

    return new DefaultKafkaReceiver<>(
      reactor.kafka.receiver.internals.ConsumerFactory.INSTANCE, 
      ReceiverOptions.<String, String>create(props).subscription(Collections.singleton("topic_ni_ian"))
    );
  }

  @Bean
  public WebClient webClient() {
    return WebClient.builder()
        // temporary hard coded
        // TODO - Use Feign
        .baseUrl("https://stream.wikimedia.org/v2/stream/recentchange")
        .build();
  }
}
