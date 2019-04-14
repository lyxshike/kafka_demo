package com.example.demo;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CustomerProducer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//以下配置都在这个类当中的e。。。ProducerConfig()
		Properties props = new Properties();
		//kafka集群
		props.put("bootstrap.servers", "localhost:9092");
		//应答级别
		props.put("acks", "all");
		//重试次数
		props.put("retries", 0);
		//批量大小
		props.put("batch.size", 16384);
		//提交延时
		props.put("linger.ms", 1);
		//缓存，producer这边可以缓存的大小
		props.put("buffer.memory", 33554432);
		// KV的序列化类
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// 自定义把消息放到某个分区中了
		props.put("partitioner.class", "com.example.demo.CustomerPartitioner");
		
		ArrayList<String> list = new ArrayList<String>();
		list.add("com.example.demo.interceptor.TimeInteceptor");
		list.add("com.example.demo.interceptor.CountInteceptor");
		
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);
		
		//构建生产者
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		
		for (int i = 20; i < 30; i++)
		    //producer.send(new ProducerRecord<String, String>("first", Integer.toString(i), Integer.toString(i)));

			//看一下调用回调函数的
			producer.send(new ProducerRecord<>("second",String.valueOf(i)), new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if(null == exception) {
						System.out.println(metadata.partition() +"--"+ metadata.offset() );
					} else {
						System.out.println("发送失败！！！");
					}
				}
			});
			
		producer.close();
		
	}

}
