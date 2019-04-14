package com.example.demo.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

public class DemoKafkaStreams {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//创建拓扑对象
		TopologyBuilder builder = new TopologyBuilder();
		
		//创建配置文件
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("application.id", "kafkaStream");
		
		//构建拓扑结构
		builder.addSource("SOURCE", "first")
		       .addProcessor("PROCESSOR", new ProcessorSupplier() {

				@Override
				public Processor get() {
					// TODO Auto-generated method stub
					return new LogProcessor();
				}
			}, "SOURCE")
		       .addSink("SINK","second","PROCESSOR");
		
		
		
		//
        KafkaStreams kafkaStreams=new KafkaStreams(builder,properties);
        
        kafkaStreams.start();
	}

}
