package tr.com.rcesur.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import tr.com.rcesur.kafka.serializer.AvroDeserializer;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


@Component
public class DynamicKafkaConsumer {
    ObjectMapper mapper = new ObjectMapper();
    @Value(value = "${kafka.bootstrapServers:localhost:9092}")
    private String bootstrapServers;
    @Value(value = "${kafka.topic.list:[]}")
    private String topicStringList;
    @Value(value = "${kafka.retention.period:1680000}")
    private String kafkaRetentionPeriod;
    private List<String> topicList;

    @PostConstruct
    public void init() throws IOException, ExecutionException, InterruptedException, ClassNotFoundException {
        topicList = getConfigurableTopicList();
        List<String> lstCurrentTopics = getKafkaTopicNameList();
        List<String> createdTopicList = topicList.stream()
                .filter(topic -> !lstCurrentTopics.stream().anyMatch(jsonTopic -> topic.equals(jsonTopic)))
                .collect(Collectors.toList());
        createNewTopicList(createdTopicList);
        updateRetentionPeriod(topicList);
        kafkaDynamicListener();
    }

    private Class getTopicClassByName(String topicName) throws JsonProcessingException, ClassNotFoundException {
        String className = (String) mapper.readValue(topicStringList, List.class)
                .stream().filter(data -> ((Map) data).containsValue(topicName))
                .map(data -> ((Map) data).get("objectClass").toString())
                .findFirst().get();
        return Class.forName(className);
    }

    private List<String> getConfigurableTopicList() throws JsonProcessingException {
        return (List<String>) mapper.readValue(topicStringList, List.class).stream()
                .map(element -> ((Map) element).get("topicName"))
                .collect(Collectors.toList());
    }

    private void updateRetentionPeriod(List<String> topicList) {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient client = AdminClient.create(config);
        for (String updatedTopicName : topicList) {
            try {
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, updatedTopicName);
                ConfigEntry retentionEntry = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, kafkaRetentionPeriod);
                Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
                updateConfig.put(resource, new Config(Collections.singleton(retentionEntry)));
                AlterConfigsResult alterConfigsResult = client.alterConfigs(updateConfig);
                alterConfigsResult.all();
            } catch (Exception e) {
                System.out.println("when topic " + updatedTopicName + " updating retention period occured error which " + e.toString());
            }
        }
    }

    private List<String> getKafkaTopicNameList() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        AdminClient client = AdminClient.create(properties);
        ListTopicsOptions topicsOptions = new ListTopicsOptions();
        topicsOptions.listInternal(true);
        Set<String> topicSet = client.listTopics(topicsOptions).names().get();
        return new ArrayList<>(topicSet);
    }

    // kafkanın dinamik bir şekilde bean oluşturma işine bakacağım

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    private void kafkaDynamicListener() throws ExecutionException, InterruptedException, JsonProcessingException, ClassNotFoundException {
        // Start brokers without using the "@KafkaListener" annotation
        Map<String, Object> consumerProps = consumerProps();
        List<KafkaMessageListenerContainer> lstContainer= new ArrayList<>();
        ContainerProperties containerProperties = new ContainerProperties(topicList.toArray(new String[0]));
        final BlockingQueue<ConsumerRecord<String, SpecificRecord>> records = new LinkedBlockingQueue<>();
        for (String topicName : topicList) {
            try {
                Class topicOfClass = getTopicClassByName(topicName);
                DefaultKafkaConsumerFactory<String,
                        AvroDeserializer> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(),
                        new AvroDeserializer(topicOfClass));
                KafkaMessageListenerContainer container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
                container.setupMessageListener((MessageListener<String, SpecificRecord>) record -> {
                    System.out.println("Message received: " + record);
                    System.out.println(record.topic());
                    records.add(record);
                });
                lstContainer.add(container);
            } catch (ClassNotFoundException e) {
                System.out.println("class not found " );
            }
        }
        lstContainer.parallelStream()
                .forEach(container-> container.start());
    }

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ramazan");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    @Bean
    public List<NewTopic> createNewTopicList(List<String> topicList) {
         return topicList.stream()
                .map(topicName ->
                        TopicBuilder.name(topicName)
                                .partitions(6)
                                .config(TopicConfig.RETENTION_MS_CONFIG, kafkaRetentionPeriod)
                                .build()
                ).collect(Collectors.toList());

    }

}
