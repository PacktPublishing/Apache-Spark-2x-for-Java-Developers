package com.packt.sfjd.ch7;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

public class TestAccumulator {

	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "E:\\hadoop");
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			 // Logger rootLogger = LogManager.getRootLogger();
			//	rootLogger.setLevel(Level.WARN); 
			
		
			
		LongAccumulator longAccumulator = sparkContext.sc().longAccumulator("ExceptionCounter");
		
		JavaRDD<String> textFile = sparkContext.textFile("src/main/resources/logFileWithException.log");		
		textFile.foreach(new VoidFunction<String>() {			
			@Override
			public void call(String line) throws Exception {
				if(line.contains("Exception")){
					longAccumulator.add(1);
					System.out.println("The intermediate value in loop "+longAccumulator.value());
					
				}				
			}
		});		
		System.out.println("The final value of Accumulator : "+longAccumulator.value());
		
		
		CollectionAccumulator<Long> collectionAccumulator = sparkContext.sc().collectionAccumulator();
		textFile.foreach(new VoidFunction<String>() {			
			@Override
			public void call(String line) throws Exception {
				if(line.contains("Exception")){
					collectionAccumulator.add(1L);
					System.out.println("The intermediate value in loop "+collectionAccumulator.value());
					
				}				
			}
		});		
		System.out.println("The final value of Accumulator : "+collectionAccumulator.value());
		
      	ListAccumulator listAccumulator=new ListAccumulator();
		
		sparkContext.sc().register(listAccumulator, "ListAccumulator");

		textFile.foreach(new VoidFunction<String>() {			
			@Override
			public void call(String line) throws Exception {
				if(line.contains("Exception")){
					listAccumulator.add("1");
					System.out.println("The intermediate value in loop "+listAccumulator.value());
					
				}				
			}
		});		
		System.out.println("The final value of Accumulator : "+listAccumulator.value());
		
	}

}
