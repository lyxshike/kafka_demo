package com.example.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class CustomerConsumer {
	
	public static void main(String[] args) {
		 Properties props = new Properties();
		 
	     props.put("bootstrap.servers", "localhost:9092");
	     //消费者组id
	     props.put("group.id", "test");
	     
	     //换组，重复消费数据
	     // 设置自动提交offset, 
	     //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); 
	     /*
	         * 为什么不是0，而是earliest呢， 因为kafka会定期清除。  从0开始做清除的。
	      * */
	     
	     //设置自动提交offset
	     props.put("enable.auto.commit", "true");
	     //提交延时
	     props.put("auto.commit.interval.ms", "1000");
	     
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); 
         consumer.subscribe(Arrays.asList("second", "first", "third"));
        // consumer.subscribe(Collections.singletonList("second"));  消费一个
         
         // 不换组， 如何重复消费数据呢
//        consumer.assign(Collections.singletonList(new TopicPartition("second",0)));
//        consumer.seek(new TopicPartition("second",0),2);
         
         while(true) {
        	ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
    	    for(ConsumerRecord<String, String> record: consumerRecords) {
    	    	System.out.println(record.topic()+ "---" + record.partition()+ "---" + record.value());
    	    }
        } 
	    
	     
	}
}
