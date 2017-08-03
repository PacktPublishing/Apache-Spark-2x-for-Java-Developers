package com.packt.sfjd.ch4;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.spark_project.guava.collect.Lists;

import scala.Tuple2;

public class ActionsExamplesOld implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\sumit.kumar\\Downloads");
		String logFile = "src/main/resources/numSeries.txt"; // Should be some file on your system
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		 /*   SparkSession spark = SparkSession
	        .builder().master("local")
	        .appName("JavaPageRank")
	         .config("spark.sql.warehouse.dir", "file:///C:/Users/sumit.kumar/Downloads/bin/warehouse")
	        .getOrCreate();
		 */   
		    SparkConf conf = new SparkConf().setMaster("local").setAppName("ApacheSparkForJavaDevelopers");
			// SparkContext context =new SparkContext(conf);
			// RDD<String> textFile = context.textFile("abc", 1);

			JavaSparkContext spark = new JavaSparkContext(conf);

		    
		  
		    JavaRDD<String> lines = spark.textFile(logFile);
			//JavaRDD<String> lines = spark.textFile(logFile).toJavaRDD().cache();
		    JavaDoubleRDD intMap= lines.mapToDouble(a-> Integer.parseInt(a)).cache();
		    JavaPairRDD<Double,Double> intDivMap= intMap.mapToPair(new PairFunction<Double, Double, Double>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Double, Double> call(Double t) throws Exception {
					
					return new Tuple2<Double,Double>(t, t%2);
				}
			});
		 
		    //	isEmpty
		     JavaRDD<Integer> intRDD = spark.parallelize(Arrays.asList(1,2,3)); 
		      boolean isRDDEmpty= intRDD.filter(a-> a.equals(5)).isEmpty();
		      System.out.println("The RDD is empty ::"+isRDDEmpty);
		    
		    //Collect		    
		    List<String> collectedList= lines.collect();
		    
		    //count()
		    long countVal=lines.count();
		    //CountByKey:
		    Map<Double, Long> countByKeyMap= intDivMap.countByKey();
		    
		    countByKeyMap.forEach(new BiConsumer<Double, Long>() {

				@Override
				public void accept( Double L, Long U ) {
                     System.out.println("The key val is 1 ::"+L);
                     System.out.println("The Long is 1 ::"+U);					
				}
			});
		    
		    
		    Map<Tuple2<Double, Double>, Long> countByValMap= intDivMap.countByValue();
		   
		    countByValMap.forEach(new BiConsumer<Tuple2<Double, Double>, Long>() {

				@Override
				public void accept( Tuple2<Double, Double> L, Long U ) {
                     System.out.println("The touple val is 1 ::"+L._1());
                     System.out.println("The touple val is 2 ::"+L._2());
                     System.out.println("The Long is 1 ::"+U);					
				}
			});
		    
		       
		    //countByValue()
		    Map<String, Long> countByVal=lines.countByValue();
		  //  max
		    intMap.max();
		    
/*		   Comparator<Double> comp =new Comparator<Double>() {
				
		    	@Override
				public int compare(Double a, Double b) {
					// TODO Auto-generated method stub
					return a.compareTo(b);
				}
			};*/
			
			intMap.max(new doubleComparator());
		    
		 /*   intMap.max(new Comparator<Double>() {
				
				@Override
				public int compare(Double a, Double b) {
					// TODO Auto-generated method stub
					return a.compareTo(b);
				}
			});
		*/    
		    intMap.max(Comparator.naturalOrder());
		    intMap.max(Comparator.reverseOrder());
		    //////check this
		  //  intMap.max(Comparator.comparing(a->a));
		    
	    	//min
		    intMap.min();
		    intMap.min(Comparator.naturalOrder());
		    
		   // First:
		   System.out.println("The first element of RDD is"+ intMap.first());
		    
		  
		    
		    //take()		    
		    List<String> takeTwo=lines.take(2);
		    takeTwo.forEach(x->System.out.println("The take elements are :: "+x));
		    
		    //	TakeOrdered:
		    List<String> takeOrderedTwo= lines.takeOrdered(2);
		    takeOrderedTwo.forEach(x->System.out.println("The takeOrdered elements are :: "+x));
		    
		    
		    //	takeOrdered(int num, java.util.Comparator<T> comp) 
		    List<String> takeCustomOrderedTwo= lines.takeOrdered(2, Comparator.reverseOrder());
		    takeCustomOrderedTwo.forEach(x->System.out.println("The takeOrdered elements with custom Comparator are :: "+x));
		     
		    
		    
			//TakeSample:
		     intRDD.takeSample(true, 3).forEach(x-> System.out.println("The take sample vals for true are :"+x));
		     intRDD.takeSample(false, 3).forEach(x-> System.out.println("The take sample vals for false are :"+x));
		     intRDD.takeSample(true, 3,9).forEach(x-> System.out.println("The take sample vals with seed  are :"+x));
		   
		     //top()
			    List<String> topFive=lines.top(5);
			    topFive.forEach(x->System.out.println("The value of top are ::"+x));
			    
			  //  top(int num, java.util.Comparator<T> comp)  
			  //  lines.top(3, Comparator.comparing(x->Integer.parseInt(x)));
		   		     
		    //reduce		    
		    Function2<String, String, Integer> reduceSumFunc = (a, b) -> (Integer.parseInt(a) + Integer.parseInt(b));
		    Double sumInt=intMap.reduce((a,b)->a+b);
		     
		    
		   /* Integer sumInt=lines.reduce(new Function2<String,String,Integer>(
		    		) {						
						@Override
						public Integer call(String a, String b) throws Exception {
							// TODO Auto-generated method stub
							return Integer.parseInt(a) + Integer.parseInt(b);
						}
					});*/

		    
		 //fold()
		    Double foldInt=intMap.fold((double) 0, (a,b)-> a+b);
		    
		 //
			//Aggeregate:
		   // 	ForEach:
		    lines.foreach(s->System.out.println(s));
		    
		    //	saveAsTextFile
		    //	saveAsObjectFile(String path) 
		    JavaRDD<String> rdd = spark.parallelize(Lists.newArrayList("1", "2"));
		    rdd.mapToPair(p -> new Tuple2<>(p, p)).saveAsObjectFile("objFileDir");
		    JavaPairRDD<String, String> pairRDD 
		        = JavaPairRDD.fromJavaRDD(spark.objectFile("objFileDir"));
		    pairRDD.collect().forEach(System.out::println);
		    
	}

 static class doubleComparator implements Comparator<Double>,Serializable{

		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

		@Override
		public int compare(Double a, Double b) {
			// TODO Auto-generated method stub
			return a.compareTo(b);
		} 
 }
	
}
