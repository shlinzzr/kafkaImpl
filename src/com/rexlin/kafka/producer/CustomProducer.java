package com.rexlin.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class CustomProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub


        Properties properties = new Properties();
        //设置kafka集群信息
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop200:9092,hadoop202:9092");
        //设置key和value的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        设置ackOs
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
//        设置尝试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);
//        设置批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16480);
//        设置发送时间
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            //生产消息，包装成kv的producerrecord发送
            ProducerRecord<String, String> record = new ProducerRecord<>("first", i + "", "message" + i); // topic, value
//            发送消息，调用send方法发送数据
            producer.send(record, new Callback() {

                @Override
                //回调函数，每次执行发送一条消息时会调用该函数，如果exception为null说明send该条消息时没有异常，
                // 可以通过metadata获取该消息的相应信息
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.topic()+ " " +  metadata.toString());
                    }
                }
            });
        }
        producer.close();
	}

}
