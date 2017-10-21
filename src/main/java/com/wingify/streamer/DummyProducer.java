package com.wingify.streamer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wingify.streamer.models.ClickStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class DummyProducer {

    public static Producer producer = null;
    private static String events[] = {"page_view", "click"};
    private static ObjectMapper mapper = new ObjectMapper();

    public static void main(String... arg) throws Exception {
        init();
        String uuids[] = new String[50];
        String sessionIds[] = new String[50];
        for(int i=0;i<50;i++){
            uuids[i] = UUID.randomUUID().toString();
            sessionIds[i]=UUID.randomUUID().toString();
        }
        int count = 1000;
        Random random = new Random();
        while (true) {
            for (int i = 0; i < 50; i++) {
                for (int j = 0; j < 50; j++) {
                    ClickStream stream = new ClickStream();
                    stream.setEvent(events[random.nextInt(2)]);
                    stream.setTimeSpent(random.nextInt(8));
                    stream.setUuid(uuids[j]);
                    stream.setSessionId(sessionIds[i]);
                    producer.send(new ProducerRecord<String, String>("raw_clicks", stream.getSessionId(), mapper.writeValueAsString(stream)));
                }
            }
            Thread.sleep(3000);
            count--;
            if (count == 0)
                break;
        }
        producer.close();
    }

    public static void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 2);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }
}
