package com.meidusa.venus.bus.util;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VenusTrafficCollector {
	private static Logger logger = LoggerFactory.getLogger("venus.bus.traffic");
	
	private AtomicLong input = new AtomicLong();
	private AtomicLong output = new AtomicLong();
	
	private AtomicLong request = new AtomicLong(); 
	private static int INTERVAL = 5; 
	private static VenusTrafficCollector instance = new VenusTrafficCollector();
	private VenusTrafficCollector(){
		new Thread(){
			{
				this.setDaemon(true);
				this.setName("Venus-Traffic-Collector");
			}
			public void run(){
				
				while(true){
					long in = input.getAndSet(0);
					long out = output.getAndSet(0);
					long re = request.getAndSet(0);
					if(logger.isInfoEnabled()){
						logger.info("timeInterval="+INTERVAL+",input="+in +",output="+out+",request="+re);
					}
					
					try {
						Thread.sleep(INTERVAL * 1000L);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
	}
	
	public void addInput(long size){
		input.addAndGet(size);
	}
	
	public void addOutput(long size){
		output.addAndGet(size);
	}
	
	public void increaseRequest(){
		request.incrementAndGet();
	}
	
	public static VenusTrafficCollector getInstance(){
		return instance;
	}
	
}
