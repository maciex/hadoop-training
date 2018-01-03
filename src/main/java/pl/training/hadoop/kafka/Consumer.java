package pl.training.hadoop.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static java.util.Collections.singletonList;

public class Consumer {

    private static String TOPIC_NAME = "messages";

    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(prepareProperties());
        kafkaConsumer.subscribe(singletonList(TOPIC_NAME));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1_000);
            records.forEach(record -> System.out.println(record.value()));
        }
    }

    private static Properties prepareProperties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp:6667");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "spark");
        return properties;
    }

}
