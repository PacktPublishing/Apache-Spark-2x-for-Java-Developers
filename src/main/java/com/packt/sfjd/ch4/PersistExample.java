/**
 * 
 */
package com.packt.sfjd.ch4;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Function;
import org.apache.spark.storage.StorageLevel;

/**
 * @author sumit.kumar
 *
 */
public class PersistExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//C:\Users\sumit.kumar\Downloads\bin\warehouse
		//System.setProperty("hadoop.home.dir", "C:\\Users\\sumit.kumar\\Downloads");
		String logFile = "src/main/resources/Apology_by_Plato.txt"; // Should be some file on your system
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
		    JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3,4,5),3).cache();	
		    JavaRDD<Integer> evenRDD= rdd.filter(new org.apache.spark.api.java.function.Function<Integer, Boolean>() {
			@Override
			public Boolean call(Integer v1) throws Exception {
			  return ((v1%2)==0)?true:false;
				}
			});
		    
		    evenRDD.persist(StorageLevel.MEMORY_AND_DISK());
		    evenRDD.foreach(new VoidFunction<Integer>() {
			@Override
			public void call(Integer t) throws Exception {
			System.out.println("The value of RDD are :"+t);
			 }
			});
		   //unpersisting the RDD 
		   evenRDD.unpersist();
		   rdd.unpersist();
		   
		   /* JavaRDD<String> lines = spark.read().textFile(logFile).javaRDD().cache();
		    System.out.println("DEBUG: \n"+ lines.toDebugString());
		   long word= lines.count();
		   JavaRDD<String> distinctLines=lines.distinct();
		   System.out.println("DEBUG: \n"+ distinctLines.toDebugString());
		   JavaRDD<String> finalRdd=lines.subtract(distinctLines);
		    
		   
		   System.out.println("DEBUG: \n"+ finalRdd.toDebugString());
		   System.out.println("The count is "+word);
		   System.out.println("The count is "+distinctLines.count());
		   System.out.println("The count is "+finalRdd.count());
		   
		   finalRdd.foreach(new VoidFunction<String>() {
			
			@Override
			public void call(String t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t);
			}
		});
*/	    /*SparkConf conf = new SparkConf().setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    StorageLevel newLevel;
		JavaRDD<String> logData = sc.textFile(logFile).cache();

	    long numAs = logData.filter(new Function(logFile, logFile, logFile, logFile, false) {
	      public Boolean call(String s) { return s.contains("a"); }
	    }).count();

	    long numBs = logData.filter(new Function(logFile, logFile, logFile, logFile, false) {
	      public Boolean call(String s) { return s.contains("b"); }
	    }).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	    
	    sc.stop();*/

	}

}
