package dev.agasen.kafkaconsumer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.DefaultKafkaReceiver;

@Configuration
public class KafkaConsumerConfig {
 
  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(
      Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
      )
    );
  }

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

}
