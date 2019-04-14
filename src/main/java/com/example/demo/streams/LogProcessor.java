package com.example.demo.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

	private ProcessorContext processorcontext;
	
	@Override
	public void init(ProcessorContext context) {
		// TODO Auto-generated method stub
		this.processorcontext = context;
	}

	@Override
	public void process(byte[] byte1, byte[] byte2) {
		// TODO Auto-generated method stub

		//获取一行数据
		String line = new String(byte2);
		
		//去除">>>"
		line = line.replaceAll(">>>", "");
		
		byte2 = line.getBytes();
		
		processorcontext.forward(byte1, byte2);
	}

	@Override
	public void punctuate(long timestamp) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	

}
