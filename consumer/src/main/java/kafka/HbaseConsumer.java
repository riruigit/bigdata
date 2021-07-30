package kafka;

import dao.HbaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import untils.PropertyUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;

public class HbaseConsumer {
    public static void main(String[] args) throws IOException, ParseException {
        // KafkaConsumer<String,String>  subscribe???
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(PropertyUtil.properties);

        kafkaConsumer.subscribe(Collections.singletonList(PropertyUtil.properties.getProperty("kafka.topic")));

        HbaseDao hbaseDao = new HbaseDao();
        while (true){
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(300);
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                String value = consumerRecord.value();
                System.out.println(value);
                hbaseDao.Put(value);
            }
        }
    }
}
