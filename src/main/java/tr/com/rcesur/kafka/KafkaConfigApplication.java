package tr.com.rcesur.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import tr.com.rcesur.kafka.producer.BatchMessageProducerExample;

@SpringBootApplication
@EnableKafka
public class KafkaConfigApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaConfigApplication.class, args);
	}


	@Autowired
	private BatchMessageProducerExample batchMessageProducerExample;

}
