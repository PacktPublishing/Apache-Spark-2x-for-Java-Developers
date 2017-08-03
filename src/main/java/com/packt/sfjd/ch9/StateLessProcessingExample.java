package com.packt.sfjd.ch9;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class StateLessProcessingExample {
	public static void main(String[] args) throws InterruptedException {

		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");

		SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("stateless Streaming Example")
				.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();

		JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(sparkSession.sparkContext()),
				Durations.milliseconds(1000));
		JavaReceiverInputDStream<String> inStream = jssc.socketTextStream("10.204.136.223", 9999);

		JavaDStream<FlightDetails> flightDetailsStream = inStream.map(x -> {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(x, FlightDetails.class);
		});
		
		
		
		//flightDetailsStream.print();
		
		//flightDetailsStream.foreachRDD((VoidFunction<JavaRDD<FlightDetails>>) rdd -> rdd.saveAsTextFile("hdfs://namenode:port/path"));
		
	   JavaDStream<FlightDetails> window = flightDetailsStream.window(Durations.minutes(5),Durations.minutes(1));
		
	    JavaPairDStream<String, Double> transfomedWindow = window.mapToPair(f->new Tuple2<String,Double>(f.getFlightId(),f.getTemperature())).
	    mapValues(t->new Tuple2<Double,Integer>(t,1))
	    .reduceByKey((t1, t2) -> new Tuple2<Double, Integer>(t1._1()+t2._1(), t1._2()+t2._2())).mapValues(t -> t._1()/t._2());
	    transfomedWindow.cache();
	    transfomedWindow.print();
	    
		jssc.start();
		jssc.awaitTermination();
	}
}
