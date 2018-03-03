package com.refactorlabs.cs378.assign11;

import org.apache.spark.Partitioner;

import scala.Tuple2;

public class CustomPartitioner extends Partitioner {

	int numOfPartitions;
	
	public CustomPartitioner(int partitions) {
		numOfPartitions = partitions;
	}
	
	@Override
	public int getPartition(Object obj) {
		Tuple2 tup = (Tuple2) obj;
		String city = (String) tup._2();
		
		if(city == null)
			city = "null";
		
		int hash = Math.abs(city.hashCode());
		
		return hash % 6;
	}

	@Override
	public int numPartitions() {
		return numOfPartitions;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CustomPartitioner) {
			if(((CustomPartitioner) obj).numPartitions() == numOfPartitions) {
				return true;
			}
		}
		
		return false;
	}

}
