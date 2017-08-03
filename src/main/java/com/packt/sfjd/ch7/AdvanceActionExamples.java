package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.deploy.worker.Sleeper;
import org.apache.spark.partial.BoundedDouble;
import org.apache.spark.partial.PartialResult;

import com.amazonaws.services.simpleworkflow.flow.worker.SynchronousActivityTaskPoller;

import scala.Tuple2;

public class AdvanceActionExamples {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		 System.setProperty("hadoop.home.dir", "E:\\hadoop");
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false").set("spark.scheduler.mode", "FAIR");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			  Logger rootLogger = LogManager.getRootLogger();
				rootLogger.setLevel(Level.WARN); 
			
			
			 //	countApprox(long timeout)
			JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20)); 			  
			PartialResult<BoundedDouble> countAprx = intRDD.countApprox(2);					
			System.out.println("Confidence::"+countAprx.getFinalValue().confidence());
			System.out.println("high::"+countAprx.getFinalValue().high());
			System.out.println("Low::"+countAprx.getFinalValue().low());
			System.out.println("Mean::"+countAprx.getFinalValue().mean());
			System.out.println("Final::"+countAprx.getFinalValue().toString());

			//countApprox(long timeout, double confidence)
			PartialResult<BoundedDouble> countAprox = intRDD.countApprox(1, 0.95);
			System.out.println("Confidence::"+countAprox.getFinalValue().confidence());
			System.out.println("high::"+countAprox.getFinalValue().high());
			System.out.println("Low::"+countAprox.getFinalValue().low());
			System.out.println("Mean::"+countAprox.getFinalValue().mean());
			System.out.println("Final::"+countAprox.getFinalValue().toString());
			
			
			    List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
			    list.add(new Tuple2<String,Integer>("a", 1));
			    list.add(new Tuple2<String,Integer>("b", 2));
			    list.add(new Tuple2<String,Integer>("c", 3));
			    list.add(new Tuple2<String,Integer>("a", 4));
		        JavaPairRDD<String,Integer> pairRDD = sparkContext.parallelizePairs(list);
		        
		      //  countByKeyApprox(long timeout);
		      PartialResult<Map<String, BoundedDouble>> countByKeyApprx = pairRDD.countByKeyApprox(1);
		      for( Entry<String, BoundedDouble> entrySet:  countByKeyApprx.getFinalValue().entrySet()){
		    	    System.out.println("Confidence for "+entrySet.getKey()+" ::"+entrySet.getValue().confidence());
					System.out.println("high for "+entrySet.getKey()+" ::"+entrySet.getValue().high());
					System.out.println("Low for "+entrySet.getKey()+" ::"+entrySet.getValue().low());
					System.out.println("Mean for "+entrySet.getKey()+" ::"+entrySet.getValue().mean());
					System.out.println("Final val for "+entrySet.getKey()+" ::"+entrySet.getValue().toString());
 
		      }
		      
		      //countByKeyApprox(long timeout, double confidence)
		      PartialResult<Map<String, BoundedDouble>> countByKeyApprox = pairRDD.countByKeyApprox(1,0.75);
		      for( Entry<String, BoundedDouble> entrySet:  countByKeyApprox.getFinalValue().entrySet()){
		    	    System.out.println("Confidence for "+entrySet.getKey()+" ::"+entrySet.getValue().confidence());
					System.out.println("high for "+entrySet.getKey()+" ::"+entrySet.getValue().high());
					System.out.println("Low for "+entrySet.getKey()+" ::"+entrySet.getValue().low());
					System.out.println("Mean for "+entrySet.getKey()+" ::"+entrySet.getValue().mean());
					System.out.println("Final val for "+entrySet.getKey()+" ::"+entrySet.getValue().toString());
 
		      }

		      //  countByKeyApprox(long timeout);
		       PartialResult<Map<Tuple2<String, Integer>, BoundedDouble>> countByValueApprx = pairRDD.countByValueApprox(1);
		      for(Entry<Tuple2<String, Integer>, BoundedDouble> entrySet : countByValueApprx.getFinalValue().entrySet()){
		    	    System.out.println("Confidence for key "+entrySet.getKey()._1() +" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().confidence());
					System.out.println("high for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().high());
					System.out.println("Low for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().low());
					System.out.println("Mean for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().mean());
					System.out.println("Final val for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().toString());
 
		      }
 
		      //  countByKeyApprox(long timeout, double confidence)
		       PartialResult<Map<Tuple2<String, Integer>, BoundedDouble>> countByValueApprox = pairRDD.countByValueApprox(1,0.85);
		      for(Entry<Tuple2<String, Integer>, BoundedDouble> entrySet : countByValueApprox.getFinalValue().entrySet()){
		    	    System.out.println("Confidence for key "+entrySet.getKey()._1() +" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().confidence());
					System.out.println("high for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().high());
					System.out.println("Low for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().low());
					System.out.println("Mean for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().mean());
					System.out.println("Final val for key "+entrySet.getKey()._1()+" and value "+ entrySet.getKey()._2()+" ::"+entrySet.getValue().toString());

		      }	
		      
		      long countApprxDistinct = intRDD.countApproxDistinct(0.95);
		      System.out.println("The approximate distinct element count is ::"+countApprxDistinct);
		      
		      JavaPairRDD<String, Long> approxCountOfKey = pairRDD.countApproxDistinctByKey(0.80);
		      approxCountOfKey.foreach(new VoidFunction<Tuple2<String,Long>>() {
				@Override
				public void call(Tuple2<String, Long> tuple) throws Exception {
					 System.out.println("The approximate distinct vlaues for Key :"+tuple._1()+" is ::"+tuple._2());					
				}
			});
		     
		      
		      
		      JavaRDD<Integer> intRDD1 = sparkContext.parallelize(Arrays.asList(1,4,3,5,7,6,9,10,11,13,16,20),4);
		      JavaRDD<Integer> intRDD2 = sparkContext.parallelize(Arrays.asList(31,34,33,35,37,36,39,310,311,313,316,320),4);
		      
		      JavaFutureAction<Long> intCount = intRDD1.countAsync();
		      System.out.println(" The async count for "+intCount);
		      
		      JavaFutureAction<List<Integer>> intCol = intRDD2.collectAsync();		     
		      for(Integer val:intCol.get()){
			      System.out.println("The collect val is "+val);			      
			      } 
		      
		      JavaFutureAction<List<Integer>> takeAsync = intRDD.takeAsync(3);
		       for( Integer val:takeAsync.get()) {
		    	   System.out.println(" The async value of take is :: "+val);
		       }
		      
		      intRDD.foreachAsync(new VoidFunction<Integer>() {				
				@Override
				public void call(Integer t) throws Exception {
					System.out.println("The val is :"+t);
					
				}
			});
		      
		      intRDD2.foreachAsync(new VoidFunction<Integer>() {					
					@Override
					public void call(Integer t) throws Exception {
						System.out.println("the val2 is :"+t);
						
					}
				});
		      
		    	     
		      
		      List<Integer> lookupVal = pairRDD.lookup("a");
		      for(Integer val:lookupVal){
		    	  System.out.println("The lookup val is ::"+val);
		      }
		      	     
		     int numberOfPartitions = intRDD2.getNumPartitions();
		     System.out.println("The no of partitions in the RDD are ::"+numberOfPartitions);
		      
		     List<Partition> partitions = intRDD1.partitions();
		     for(Partition part :partitions){
		    	 System.out.println(part.toString());
		    	 System.out.println(part.index());
		    	
		    
		     }

		     List<Integer>[] collectPart = intRDD1.collectPartitions(new int[]{1,2});
		     System.out.println("The length of collectPart is "+collectPart.length);		     
		     
		     for(List<Integer> i:collectPart){
		    	 for(Integer it:i){
		    		 System.out.println(" The val of collect is "+it);
		    	 }
		    	 
		     }
		     
		     System.out.println(" The no of partitions are ::"+ intRDD1.getNumPartitions());
		    
	}

}
