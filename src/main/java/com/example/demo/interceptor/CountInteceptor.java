package com.example.demo.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountInteceptor implements ProducerInterceptor<String, String> {

	private int successCount = 0;
	private int errorCount = 0;
	
	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		// TODO Auto-generated method stub
		
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if(exception == null) {
			successCount++;
		} else {
			errorCount++;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		//知道为什么这个次数， 要写在close方法。  从interceptor的声明周期来看
		System.out.println("发送成功的次数" + successCount);
		System.out.println("发送失败的次数"+ errorCount);
	}

}
