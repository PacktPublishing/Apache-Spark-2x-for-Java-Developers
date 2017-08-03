package com.packt.sfjd.ch9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class FileStreamingEx {
	
	 public static void main(String[] args) {
	    	//Window Specific property if Hadoop is not instaalled or HADOOP_HOME is not set
			 System.setProperty("hadoop.home.dir", "E:\\hadoop");
	    	//Logger rootLogger = LogManager.getRootLogger();
	   		//rootLogger.setLevel(Level.WARN); 
	        SparkConf conf = new SparkConf().setAppName("KafkaExample").setMaster("local[*]");
	        String inputDirectory="E:\\hadoop\\streamFolder\\";
	     
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(1));
	       // streamingContext.checkpoint("E:\\hadoop\\checkpoint");
	        Logger rootLogger = LogManager.getRootLogger();
	   		rootLogger.setLevel(Level.WARN); 
	   		
	   		JavaDStream<String> streamfile = streamingContext.textFileStream(inputDirectory);
	   		streamfile.print();
	   		streamfile.foreachRDD(rdd-> rdd.foreach(x -> System.out.println(x)));
	   		
	   			   		
	   		JavaPairDStream<LongWritable, Text> streamedFile = streamingContext.fileStream(inputDirectory, LongWritable.class, Text.class, TextInputFormat.class);
	   	 streamedFile.print();
	   		
	   	 streamingContext.start();
	   	 

	        try {
				streamingContext.awaitTermination();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	 }

}
