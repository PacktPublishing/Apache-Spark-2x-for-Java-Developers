package com.packt.sfjd.ch9;

import java.util.Arrays;
import java.util.List;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountSocketStateful {
	 

	  public static void main(String[] args) throws Exception {
		  System.setProperty("hadoop.home.dir", "E:\\hadoop");
			
		    SparkConf sparkConf = new SparkConf().setAppName("WordCountSocketEx").setMaster("local[*]");
		    JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		    streamingContext.checkpoint("E:\\hadoop\\checkpoint");
		 // Initial state RDD input to mapWithState
		    @SuppressWarnings("unchecked")
		    List<Tuple2<String, Integer>> tuples =Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
		    JavaPairRDD<String, Integer> initialRDD = streamingContext.sparkContext().parallelizePairs(tuples);
		    
		    JavaReceiverInputDStream<String> StreamingLines = streamingContext.socketTextStream( "10.0.75.1", Integer.parseInt("9000"), StorageLevels.MEMORY_AND_DISK_SER);
		    
		    JavaDStream<String> words = StreamingLines.flatMap( str -> Arrays.asList(str.split(" ")).iterator() );
		   
		    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(str-> new Tuple2<>(str, 1)).reduceByKey((count1,count2) ->count1+count2 );
		   
		 

	    // Update the cumulative count function
	    Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
	        new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
	          @Override
	          public Tuple2<String, Integer> call(String word, Optional<Integer> one,
	              State<Integer> state) {
	            int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
	            Tuple2<String, Integer> output = new Tuple2<>(word, sum);
	            state.update(sum);
	            return output;
	          }
	        };

	    // DStream made of get cumulative counts that get updated in every batch
	    JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream = wordCounts.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

	    stateDstream.print();
	    streamingContext.start();
	    streamingContext.awaitTermination();
	  }
	}