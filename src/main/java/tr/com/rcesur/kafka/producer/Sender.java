package tr.com.rcesur.kafka.producer;

import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Created by ramazancesur on 29/04/2020.
 */
public class Sender {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

    @Autowired
    private KafkaTemplate<String,SpecificRecord> kafkaTemplate;

    public void send(String topic, SpecificRecord payload){
        LOGGER.info("Sending payload='{}' to topic='{}' with key='{}'", payload, topic);
        kafkaTemplate.send(topic,payload);
    }

}
