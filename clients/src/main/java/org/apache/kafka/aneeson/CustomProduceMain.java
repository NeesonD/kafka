package org.apache.kafka.aneeson;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author daile
 * @version 1.0
 * @date 2020/4/19 12:00
 */
public class CustomProduceMain {

    public static final String BROKER_LIST = "localhost:9092";
    public static final String topic = "hidden-topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", BROKER_LIST);
        properties.put("interceptor.classes", CustomProducerInterceptor.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "msg-" + i);
            producer.send(producerRecord).get();
        }
        producer.close();
    }

}
