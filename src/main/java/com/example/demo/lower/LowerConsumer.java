package com.example.demo.lower;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;  
public class LowerConsumer {
	public static void main(String[] args) {
		//定义相关的参数
		
		//kafka集群
		ArrayList<String> brokers = new ArrayList<String>();
		brokers.add("localhost");
//		brokers.add("localhost:9093");
//		brokers.add("localhost:9094");
		
		//端口
		int port = 9093;
		
		//主题
		String topic = "second";
		
		//分区
		int partition = 0;
		
		//offset
		long offset = 2L;
	
		LowerConsumer lowerConsumer  = new LowerConsumer();
		lowerConsumer.getData(brokers, port, topic, partition, offset);
	} 
	
	
	
	//找分区领导
	private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
		for(String broker: brokers) {
			//创建获取分区leader 的消费者对象
			SimpleConsumer getLeader=	new SimpleConsumer(broker,port,1000,1024*4, "getleader");
		    TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
		    
		    //获取主题元数据返回值
		    TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
		    
		    //解析元数据返回值 
		    List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
		    
		    //遍历主题元数据
		    for(TopicMetadata topicMetada: topicsMetadata) {
		    	//获取多个分区的元数据信息
		    	List<PartitionMetadata> partitionsMetadata = topicMetada.partitionsMetadata();
		    	//遍历分区元数据
		    	for(PartitionMetadata partitionMetadata: partitionsMetadata) {
		    		if(partition == partitionMetadata.partitionId()) {
		    			return partitionMetadata.leader();
		    		}
		    	}
		    }
		    
		}
		
		return null;
	}
	
	 //获取数据
	private void getData(List<String> brokers, int port, String topic, int partition, Long offset) {
		
		//获取分区的leader
		BrokerEndPoint leader = findLeader(brokers,port,topic,partition);
		if(leader == null) {
			return;
		}
		
		String leaderHost = leader.host();
		//获取数据的消费者对象
		SimpleConsumer getData = new SimpleConsumer(leaderHost, port, 1000, 1024*4, "getData");
		//创建获取数据的对象(下行可以添加多个addfetch)
	    FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 5000000).build();
		
	    //获取数据返回值
		FetchResponse fetchResponse = getData.fetch(fetchRequest);
		
		//解析返回值(为什么传topic,partition， 上面的addfetch)
		ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
		//遍历并打印
		for(MessageAndOffset messageAndOffset : messageAndOffsets) {
			long offset1 = messageAndOffset.offset();
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			System.out.println(offset1+"--"+new String(bytes));
		}
	}
	
	
}
