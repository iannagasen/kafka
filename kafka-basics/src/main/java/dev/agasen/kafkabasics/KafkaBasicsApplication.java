package dev.agasen.kafkabasics;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Producer;
import org.springframework.context.annotation.Bean;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class KafkaBasicsApplication {

	public static void main(String[] args) throws Exception {
		SpringApplication.run(KafkaBasicsApplication.class, args);
		/*
		 * TO RUN CONSUMER/s AND PRODUCER/S SIMULTANEOUSLY -- RUN IN SEPARATE TERMINAL
		 * ./mvnw spring-boot:run -DskipTests=true -Dspring-boot.run.arguments="CONSUMER"
		 * ./mvnw spring-boot:run -DskipTests=true -Dspring-boot.run.arguments="PRODUCER"
		 */
		for (String arg : args) {
			log.info("HELLO");
			log.info(arg);
			if ("CONSUMER".equals(arg)) {
				kafkaConsumerWithGracefulShutdown().run();
			} else if ("CONSUMER2".equals(arg)) {
				consumerInConsumerGroups().run();
			} else {
				log.info("NO METHOD TO RUN...");	
			}
		}
	}

	// @Bean
	public static CommandLineRunner usingKafkaToProduceManually() {
		return args -> {
			// Create Producer Properties
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", StringSerializer.class.getName());

			// Create the Producer
			// String, String => K, V ; must match the serializer - deserializer
			KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
			
			// Create a Producer record
			// hello world will be send to a topic named demo_top
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", "hello world");

			// Send Data
			producer.send(producerRecord);

			// tell the producer to send all data block until done -- synchronous
			producer.flush();

			// close the producer
			producer.close();
		};
	}

	// @Bean
	public static CommandLineRunner producerWithCallbacks() {
		return args -> {
			log.info("Im a Kafka Producer");

			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", StringSerializer.class.getName());

			KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
			
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic", "hello world");

			// Producer with callback, Callback is called onCompletion
			// Callback is executed everytime a records is:
			// 		- successfully sent
			// 		- exception is thrown
			producer.send(producerRecord, (metadata, exception) -> {
				if (exception == null) { // record is successfully sent
					printMetadata((metadata));
				} else {
					log.error("Error while producing", exception);
				}
			});

			producer.flush();
			producer.close();
		};
	}

	// @Bean
	public static CommandLineRunner producerWithCallbackAndMultipleMessage() {
		return args -> {
			log.info("Im a Kafka Producer");

			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", StringSerializer.class.getName());

			KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
			
			// Producing Multiple Messages
			IntStream.range(0, 30).forEach(i -> {
				ProducerRecord<String, String> producerRecord = 
						new ProducerRecord<>("demo_topic", "hello world" + i);
				producer.send(producerRecord, (metadata, exception) -> {
					if (exception == null) { // record is successfully sent
						printMetadata((metadata));
					} else {
						log.error("Error while producing", exception);
					}
				});
			});
			producer.flush();
			producer.close();
		};
	}

	// @Bean
	public static CommandLineRunner producerWithCallbackAndMultipleMessageUsingBatching() {
		return args -> {
			log.info("Im a Kafka Producer");

			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
			properties.setProperty("key.serializer", StringSerializer.class.getName());
			properties.setProperty("value.serializer", StringSerializer.class.getName());

			// Set a batch a size - no. of records that a Producer sends in a single batch to the broker
			properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");

			KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
			
			// Outer loop - for batching 1 batch = 30 records batch into 1 partition
			IntStream.range(0, 10).forEach(j -> {
				// Inner Loop - Since this is done very quickly
				// Kafka treat this as a batch - STICKY PARTITIONING
				IntStream.range(0, 30).forEach(i -> {
					ProducerRecord<String, String> producerRecord = 
							new ProducerRecord<>("demo_topic", "hello world" + i);
					producer.send(producerRecord, (metadata, exception) -> {
						if (exception == null) { 
							printMetadata((metadata));
						} else {
							log.error("Error while producing", exception);
						}
					});
				});

				// add delay, this is to make Kafka will not batch the next 30 records/message
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					log.error("Error: ", e);
				}
			});

			producer.flush();
			producer.close();
		};
	}

	// @Bean
	public static CommandLineRunner producerWithKeys() {
		return args -> {
			Properties props = new Properties();
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			KafkaProducer<String, String> producer = new KafkaProducer<>(props);

			// Outer loop is to produce multiple records with same key - 3 records in a key
			IntStream.range(0, 2).forEachOrdered(j -> {
				IntStream.range(0, 10).forEachOrdered(i -> {
					String topic = "demo_topic";
					String key = "id_" + i;
					String value = "hello world " + i;
					
					ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
					producer.send(producerRecord, (metadata, exception) -> {
						if (exception == null) {
							log.info("Key: %s | Partition: %s".formatted(key, metadata.partition()));
						} else {
							log.error("Error while producing", exception);
						}
					});
				});

				// add delay to separate the 3 batches
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
				
			producer.close();
			producer.flush();
		};
	}

	// @Bean
	public static CommandLineRunner kafkaConsumer() {
		return args -> {
			Properties props = new Properties();

			String groupId = "ian-application";
			String topic = "demo_topic";
			
			// Producer Config
			// Producer -> Serialize to a lightweight format suitable for transportation (bytes)
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			// Consumer Config 
			// Consumer -> Deserialize bytes to a readable format (String)
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

			// auto.offset.reset - 
			// possible values:
			// 	-	none - if we have no existing consumer group -> FAIL. need to setup consumer group before starting up
			// 	- earliest - read from the beginning of topic
			// 	- latest - read the new message from now
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
				// subscribe to a topic
				consumer.subscribe(List.of(topic));

				// poll for data (infinitely for now)
				while (true) {
					log.info("Polling");

					// pass a duration - how long are we willing to wait for data
					// reset when data is received
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
					
					records.forEach(rec -> {
						log.info(
							"Key: %s, Value: %s, Partition: %s, Offset: %s"
							.formatted(rec.key(), rec.value(), rec.partition(), rec.offset())
						);
					});
				}
			}

		};
	}

	// @Bean
	public static CommandLineRunner kafkaConsumerWithGracefulShutdown() {
		return args -> {
			String groupId = "ian-application";
			String topic = "demo_topic";

			Properties props = new Properties();
			
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
			
			// get a reference for the main thread
			final Thread mainThread = Thread.currentThread();
			
			// adding the shutdown hook - trick to exit the while loop
			// basically just a listener - "onShutdown" ^C
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");

				// Trigger a wake up exception in the while(true) loop
				consumer.wakeup();
				
				// join the main thread to allow the execution of the code in the main thread
				try {
					mainThread.join();
				} catch (InterruptedException e) {
					log.error("ERROR: ", e);
				}
			}));
			
			try {
				consumer.subscribe(List.of(topic));
				while (true) {
					log.info("Polling");
					// consumer.poll() will trigger the WakeUpException
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
					records.forEach(rec -> {
						log.info(
							"Key: %s, Value: %s, Partition: %s, Offset: %s"
							.formatted(rec.key(), rec.value(), rec.partition(), rec.offset())
						);
					});
				}
			} catch (WakeupException e) {
				log.info("Consumer is starting to shut down");
			} catch (Exception e) {
				log.error("Unexpected Exception", e);
			} finally {
				consumer.close();
				log.info("The consumer is now gracefully shutdown");
			}
		};
	}

	// @Bean 
	public static CommandLineRunner consumerInConsumerGroups() {
		return args -> {
			String groupId = "ian-application";
			String topic = "demo_topic";

			Properties props = new Properties();
			
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

			final Thread mainThread = Thread.currentThread();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
				consumer.wakeup();

				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}));

			try {
				consumer.subscribe(List.of(topic));
				while (true) {
					log.info("Polling");
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
					consumerRecords.forEach(rec -> {
						log.info(
							"Key: %s, Value: %s, Partition: %s, Offset: %s"
							.formatted(rec.key(), rec.value(), rec.partition(), rec.offset())
						);
					});
				}
			} catch (WakeupException e) {
				log.info("Consumer is starting to shut down");
			} catch (Exception e) {
				log.error("Unexpected Exception", e);
			} finally {
				consumer.close();
			}
		};
	}

	@Bean CommandLineRunner consumerDemoCooperative() {
		return args -> {
			String groupId = "ian-application";
			String topic = "demo_topic";

			Properties props = new Properties();
			
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
			props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

			// CooperativeStickyAssignor
			props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

			KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

			final Thread mainThread = Thread.currentThread();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Detected a shutdown, lets exit by calling consumer.wakeup()...");
				consumer.wakeup();

				try {
					mainThread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}));

			try {
				consumer.subscribe(List.of(topic));
				while (true) {
					log.info("Polling");
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
					consumerRecords.forEach(rec -> {
						log.info(
							"Key: %s, Value: %s, Partition: %s, Offset: %s"
							.formatted(rec.key(), rec.value(), rec.partition(), rec.offset())
						);
					});
				}
			} catch (WakeupException e) {
				log.info("Consumer is starting to shut down");
			} catch (Exception e) {
				log.error("Unexpected Exception", e);
			} finally {
				consumer.close();
			}

		};
	}

	public static void printMetadata(RecordMetadata m) {
		log.info(
			"""
			\nReceived new metadata 
			Topic: %s
			Key: %s
			Partition: %s
			Offset: %s
			Timestamp: %s
			"""
			.formatted(m.topic(), m.partition(), m.offset(), m.timestamp())
		);
	}

}
