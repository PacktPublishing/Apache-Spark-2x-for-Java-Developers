package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class PartitionIndexInformation {
public static void main(String[] args) {
	
	SparkSession sparkSession = SparkSession.builder().master("local").appName("My App")
			.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();

	JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
	
	
	JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(
			Arrays.asList(new Tuple2<Integer, String>(1, "A"),new Tuple2<Integer, String>(2, "B"),
					new Tuple2<Integer, String>(3, "C"),new Tuple2<Integer, String>(4, "D"),
					new Tuple2<Integer, String>(5, "E"),new Tuple2<Integer, String>(6, "F"),
					new Tuple2<Integer, String>(7, "G"),new Tuple2<Integer, String>(8, "H")));
	
	JavaPairRDD<Integer, String> partitionBy = pairRdd.partitionBy(new HashPartitioner(2));
	
	JavaPairRDD<Integer, String> mapValues = partitionBy.mapToPair(t -> new Tuple2<Integer, String>(t._1()+1,t._2()));
	
	
	JavaRDD<String> mapPartitionsWithIndex =partitionBy.mapPartitionsWithIndex((index, iterator) -> {
		List<String> list =new ArrayList<>();
		
		while(iterator.hasNext()){
			Tuple2<Integer, String> next = iterator.next();
			list.add(index+":"+next._1()+":"+next._2());
		}
		
		return list.iterator();
	},false);
	
	JavaRDD<String> mapPartitionsWithIndex1 =mapValues.mapPartitionsWithIndex((index, iterator) -> {
		List<String> list =new ArrayList<>();
		
		while(iterator.hasNext()){
			Tuple2<Integer, String> next = iterator.next();
			list.add(index+":"+next._1()+":"+next._2());
		}
		
		return list.iterator();
	},false);
	
	System.out.println(mapPartitionsWithIndex.collect());
	System.out.println(mapPartitionsWithIndex1.collect());
}
}
