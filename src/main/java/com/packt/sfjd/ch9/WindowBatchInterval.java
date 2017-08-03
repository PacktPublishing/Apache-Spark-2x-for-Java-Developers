package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WindowBatchInterval {
	
	 public static void main(String[] args) {
	    	//Window Specific property if Hadoop is not instaalled or HADOOP_HOME is not set
			 System.setProperty("hadoop.home.dir", "E:\\hadoop");
	    	//Logger rootLogger = LogManager.getRootLogger();
	   		//rootLogger.setLevel(Level.WARN); 
	        SparkConf conf = new SparkConf().setAppName("KafkaExample").setMaster("local[*]");
	        
	     
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.minutes(2));
	        streamingContext.checkpoint("E:\\hadoop\\checkpoint");
	        Logger rootLogger = LogManager.getRootLogger();
	   		rootLogger.setLevel(Level.WARN); 
	   		
	   	 List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 10), new Tuple2<>("world", 10));
		    JavaPairRDD<String, Integer> initialRDD = streamingContext.sparkContext().parallelizePairs(tuples);
				    

		    JavaReceiverInputDStream<String> StreamingLines = streamingContext.socketTextStream( "10.0.75.1", Integer.parseInt("9000"), StorageLevels.MEMORY_AND_DISK_SER);
		    
		    JavaDStream<String> words = StreamingLines.flatMap( str -> Arrays.asList(str.split(" ")).iterator() );
		   
		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(str-> new Tuple2<>(str, 1)).reduceByKey((count1,count2) ->count1+count2 );
		   
		    wordCounts.print();
		    wordCounts.window(Durations.minutes(8)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    wordCounts.window(Durations.minutes(8),Durations.minutes(2)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    wordCounts.window(Durations.minutes(12),Durations.minutes(8)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    wordCounts.window(Durations.minutes(2),Durations.minutes(2)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    wordCounts.window(Durations.minutes(12),Durations.minutes(12)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
	       
		    //comment these two operation to make it run
		    wordCounts.window(Durations.minutes(5),Durations.minutes(2)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
		    wordCounts.window(Durations.minutes(10),Durations.minutes(1)).countByValue()
	       .foreachRDD(tRDD -> tRDD.foreach(x->System.out.println(new Date()+" ::The window count tag is ::"+x._1() +" and the val is ::"+x._2())));
	       
	        streamingContext.start();
	        try {
				streamingContext.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 }

}
