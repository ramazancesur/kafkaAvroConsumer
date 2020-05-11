package tr.com.rcesur.kafka.producer;

import io.codearte.jfairy.Fairy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tr.com.rcesur.kafka.avromodels.Address;
import tr.com.rcesur.kafka.avromodels.Person;

import java.util.Locale;
import java.util.stream.IntStream;

/**
 * Created by ramazancesur on 29/04/2020.
 */
@Component
@EnableScheduling
public class BatchMessageProducerExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchMessageProducerExample.class);
    private final Fairy fairy = Fairy.create(Locale.ENGLISH);

    @Autowired
    private Sender sender;


    @Value("${kafka.topic.list:[]}")
    private String topicName;

    @Scheduled(fixedDelay = 1000)
    public void execute() {
        LOGGER.info("BatchMessageConsumingExample is executing...");
        IntStream.rangeClosed(1, 10).boxed()
                .map(i -> fairy.person())
                .forEach(f -> {
                    Person person = Person.newBuilder()
                            .setFirstName(f.getFirstName())
                            .setLastName(f.getLastName())
                            .build();
                    sender.send("cesur", person);
                    Address address = Address.newBuilder()
                            .setZip(f.getAddress().getPostalCode())
                            .setCity(f.getAddress().getCity())

                            .setStreet(f.getAddress().getStreet())
                            .setStreetNumber(f.getAddress().getStreetNumber())
                            .build();

                    LOGGER.info("producing {}, {}", person, address);
                    sender.send("ramazan", address);
                });
    }

}

