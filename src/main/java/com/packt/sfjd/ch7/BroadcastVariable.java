package com.packt.sfjd.ch7;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

public class BroadcastVariable {

	public static void main(String[] args) {
		
	
//	SparkConf conf = new SparkConf().setMaster("local").setAppName("BroadCasting");
//	JavaSparkContext jsc = new JavaSparkContext(conf);
//	
//	Broadcast<String> broadcastVar = jsc.broadcast("Hello Spark");
//	
	 SparkSession sparkSession = SparkSession.builder().master("local").appName("My App")
			 .config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();
	 
	 Broadcast<String> broadcastVar= sparkSession.sparkContext().broadcast("Hello Spark",  scala.reflect.ClassTag$.MODULE$.apply(String.class));
	 System.out.println(broadcastVar.getValue());
	 
	 broadcastVar.unpersist();
	// broadcastVar.unpersist(true);
	 broadcastVar.destroy();
	
	}
}
