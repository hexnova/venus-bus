package com.meidusa.venus.pool;

public class SystemTime {

	public static void main(String[] args) {
		long start = java.lang.System.currentTimeMillis();
		
		for(int i=0;i< 100000;i++){
			java.lang.System.currentTimeMillis();
		}
		long end = java.lang.System.currentTimeMillis();
		java.lang.System.out.println(end - start);
		
	}

}
