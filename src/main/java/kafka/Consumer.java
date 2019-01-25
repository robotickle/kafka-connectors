package kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.22.67.230:9092");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.20.67.224:9092,192.20.67.225:9092,192.20.67.226:9092");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "BT-RPP");
        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("impression"));
        
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(10);
            if (recs.count() == 0) {
                System.out.println("not received....");
            } else {
                for (ConsumerRecord<String, String> rec : recs) {
                    System.out.printf("Recieved %s: %s", rec.key(), rec.value());
                }
            }
        }
    }
}