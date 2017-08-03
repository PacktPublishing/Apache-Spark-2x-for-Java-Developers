package com.packt.sfjd.ch7;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner{

	/**
	 * 
	 */
	private static final long serialVersionUID = -7397874438301367044L;

	final int maxPartitions=2;
	
	@Override
	public int getPartition(Object key) {
				
		return (((String) key).length()%maxPartitions);
		
		
	}

	@Override
	public int numPartitions() {
		
		return maxPartitions;
	}

}
