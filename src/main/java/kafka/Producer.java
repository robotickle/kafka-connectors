package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.22.67.230:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<String, String> data;
            if (i % 2 == 0) {
                data = new ProducerRecord<String, String>("impression", 0, Integer.toString(i), String.format("%d BTT - RPP Even Test", i));
            } else {
                data = new ProducerRecord<String, String>("impression", 0, Integer.toString(i), String.format("%d BTT - RPP Odd Test", i));
            }
            producer.send(data);
            Thread.sleep(1L);
        }
        producer.close();
    }
}