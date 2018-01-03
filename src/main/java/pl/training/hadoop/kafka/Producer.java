package pl.training.hadoop.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer<T> {

    private static String TOPIC_NAME = "messages";

    private KafkaProducer<String, T> kafkaProducer;

    public Producer() {
        kafkaProducer = new KafkaProducer<>(prepareProperties());
    }

    public Producer(Properties properties) {
        kafkaProducer = new KafkaProducer<>(properties);
    }

    private Properties prepareProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "sandbox-hdp:6667");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void send(T message) {
        kafkaProducer.send(new ProducerRecord<>(TOPIC_NAME, message));
    }

}
