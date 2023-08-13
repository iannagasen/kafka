package dev.agasen.wikimedia;

import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WikimediaApplication {

	public static void main(String[] args) {
		SpringApplication.run(WikimediaApplication.class, args);
	}

	public CommandLineRunner run() {
		return args -> {
		};
	}

}
