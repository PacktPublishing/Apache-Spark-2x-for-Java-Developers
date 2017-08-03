package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class WordCountSocketJava8Ex {
	 
	  public static void main(String[] args) throws Exception {
	   
        System.setProperty("hadoop.home.dir", "E:\\hadoop");
	
	    SparkConf sparkConf = new SparkConf().setAppName("WordCountSocketEx").setMaster("local[*]");
	    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("hello", 10), new Tuple2<>("world", 10));
	    JavaPairRDD<String, Integer> initialRDD = streamingContext.sparkContext().parallelizePairs(tuples);
			    

	    JavaReceiverInputDStream<String> StreamingLines = streamingContext.socketTextStream( "10.0.75.1", Integer.parseInt("9000"), StorageLevels.MEMORY_AND_DISK_SER);
	    
	    JavaDStream<String> words = StreamingLines.flatMap( str -> Arrays.asList(str.split(" ")).iterator() );
	   
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(str-> new Tuple2<>(str, 1)).reduceByKey((count1,count2) ->count1+count2 );
	   
	    wordCounts.print();
	    
	  JavaPairDStream<String, Integer> joinedDstream = wordCounts.transformToPair(
			   new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
				    @Override public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> rdd) throws Exception {
				    	rdd.join(initialRDD).mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>, String, Integer>() {
							@Override
							public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> joinedTuple)
									throws Exception {
								// TODO Auto-generated method stub
								return new Tuple2<>( joinedTuple._1(), (joinedTuple._2()._1()+joinedTuple._2()._2()) );
							}
						});
					
					return rdd; 				     
				    }
				  });
	   
	  joinedDstream.print();
	    streamingContext.start();
	    streamingContext.awaitTermination();
	  }
	}