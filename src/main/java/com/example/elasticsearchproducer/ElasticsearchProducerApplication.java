package com.example.elasticsearchproducer;

import com.github.javafaker.Faker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

@SpringBootApplication
public class ElasticsearchProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchProducerApplication.class, args);
    }

    private Faker faker = new Faker(Locale.GERMAN);
    Stream<Person> personStream = Stream.generate(() -> createPerson());
    private AtomicLong counter = new AtomicLong();

    private Person createPerson() {
        return new Person(faker.name().firstName(),
                faker.name().firstName(),
                faker.name().lastName(),
                faker.chuckNorris().fact());
    }

    @Bean
    public Supplier<Flux<Message<Person>>> persons() {
        return () -> Flux.fromStream(personStream)
                .map(person -> MessageBuilder.withPayload(person).setHeader(KafkaHeaders.MESSAGE_KEY, counter.incrementAndGet()).build())
                .delayElements(Duration.ofMillis(10));
    }

}

record Person(String firstName, String middleName, String lastName, String favouriteQuote) {}
