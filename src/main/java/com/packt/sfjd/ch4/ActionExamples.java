package com.packt.sfjd.ch4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import scala.collection.generic.BitOperations.Int;

public class ActionExamples {

	public static void main(String[] args) {
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		 //	isEmpty
		  JavaRDD<Integer> intRDD = sparkContext.parallelize(Arrays.asList(1,4,3)); 
/*		  boolean isRDDEmpty =  intRDD.filter(new Function<Integer, Boolean>() {
			
			@Override
			public Boolean call(Integer v1) throws Exception {
				return v1.equals(5);
			}
		}).isEmpty();	*/
		  boolean isRDDEmpty= intRDD.filter(a-> a.equals(5)).isEmpty();
	    System.out.println("The RDD is empty ::"+isRDDEmpty);
	      
 
	  //Collect		    
	    List<Integer> collectedList= intRDD.collect();
	    for(Integer elements: collectedList){
	    	System.out.println( "The collected elements are ::"+elements);
	    }
	    

        // collectAsMap
	    List<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String,Integer>>();
	    list.add(new Tuple2<String,Integer>("a", 1));
	    list.add(new Tuple2<String,Integer>("b", 2));
	    list.add(new Tuple2<String,Integer>("c", 3));
	    list.add(new Tuple2<String,Integer>("a", 4));
        JavaPairRDD<String,Integer> pairRDD = sparkContext.parallelizePairs(list);
        Map<String, Integer> collectMap=pairRDD.collectAsMap();
	    for( Entry<String, Integer> entrySet :collectMap.entrySet() ){
	    	System.out.println("The key of Map is : "+entrySet.getKey()+" and the value is : "+entrySet.getValue());	    	
	    }
	    
	    //count()
	    long countVal=intRDD.count();
	    System.out.println("The number of elements in the the RDD are :"+countVal);
	    
	    
	  //CountByKey
	    Map<String, Long> countByKeyMap= pairRDD.countByKey();
	    for( Entry<String, Long> entrySet :countByKeyMap.entrySet() ){
	    	System.out.println("The key of Map is : "+entrySet.getKey()+" and the value is : "+entrySet.getValue());	    	
	    } 
	      
	  //countByValue  
	    Map<Tuple2<String, Integer>, Long> countByValueMap= pairRDD.countByValue();
	    for( Entry<Tuple2<String, Integer>, Long> entrySet :countByValueMap.entrySet() ){
	    	System.out.println("The key of Map is a tuple having value : "+entrySet.getKey()._1()+" and "+entrySet.getKey()._2()+" and the value of count is : "+entrySet.getValue());	    	
	    } 
	    
	    
	    //  max
	  // Integer maxVal=intRDD.max(new integerComparator());
	 //  System.out.println("The max value of RDD is "+maxVal);
	    Integer maxVal=intRDD.max(Comparator.naturalOrder());
	    System.out.println("The max value of RDD is "+maxVal);
	    
	    
	  //min
	   // Integer minVal=intRDD.min(new integerComparator()); 
	  //  System.out.println("The min value of RDD is "+minVal);
	    Integer minVal=intRDD.min(Comparator.naturalOrder()); 
	    System.out.println("The min value of RDD is "+minVal);
	    
	    //first
	    Integer first=intRDD.first();
	    System.out.println("The first value of RDD is "+first);
	    
	    //take()		    
	    List<Integer> takeTwo=intRDD.take(2);
	    for(Integer intVal:takeTwo){
	    	System.out.println("The take elements are :: "+intVal);
	    }
	
      //TakeOrdered:
	    List<Integer> takeOrderedTwo= intRDD.takeOrdered(2);
	    for(Integer intVal:takeOrderedTwo){
	    	System.out.println("The take ordered elements are :: "+intVal);
	    }
	    
	    //TakeOrdered
	   List<Integer> takeCustomOrderedTwo= intRDD.takeOrdered(2, Comparator.reverseOrder());
	   // List<Integer> takeCustomOrderedTwo= intRDD.takeOrdered(2, new integerReverseComparator());
	    for(Integer intVal:takeCustomOrderedTwo){
	    	System.out.println("The take custom ordered elements are :: "+intVal);
	    }
	    
	    //TakeSample:
	    List<Integer> takeSamepTrueList= intRDD.takeSample(true, 4);
	    for(Integer intVal:takeSamepTrueList){
	    	System.out.println("The take sample vals for true are : "+intVal);
	    }

	    List<Integer> takeSamepFalseList=intRDD.takeSample(false, 4);
	    for(Integer intVal:takeSamepFalseList){
	    	System.out.println("The take sample vals for false are : "+intVal);
	    } 

	    
	    
	    List<Integer> takeSamepTrueSeededList=intRDD.takeSample(true, 4,9);
	    for(Integer intVal:takeSamepTrueSeededList){
	    	System.out.println("The take sample vals with seed  are : "+intVal);
	    }
    
	    //Top
	  //  List<Integer> topTwo=intRDD.top(2); 
	    
	  //  List<Integer> topTwo=intRDD.top(2,new integerReverseComparator());   
	    
	    List<Integer> topTwo=intRDD.top(2,Comparator.reverseOrder());	   
	    for(Integer intVal:topTwo){
	    	System.out.println("The top two values of the RDD are : "+intVal);
	    }
	   
	    //reduce	    
	    /*Integer sumInt=intRDD.reduce(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
			return v1+v2;
			}
		});*/
	   
	    Integer sumInt=intRDD.reduce((a,b)->a+b);
	    System.out.println("The sum of all the elements of RDD using reduce is "+sumInt);
	   
	    //fold()
	  /*  Integer foldInt=intRDD.fold(0, new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});*/
	    Integer foldInt=intRDD.fold(0, (a,b)-> a+b);
	    System.out.println("The sum of all the elements of RDD using fold is "+foldInt);
	    
	    //forEach
	    intRDD.foreach(new VoidFunction<Integer>() {
		@Override
	    public void call(Integer t) throws Exception {
			System.out.println("The element values of the RDD are ::"+t);				
			}
		});
	    
	    intRDD.foreach(x->System.out.println("The element values of the RDD are ::"+x));
	    
	    //saveAsTextFile
	    intRDD.saveAsTextFile("TextFileDir");
	    JavaRDD<String> textRDD= sparkContext.textFile("TextFileDir");
	    textRDD.foreach(new VoidFunction<String>() {
		@Override
			public void call(String x) throws Exception {
			System.out.println("The elements read from TextFileDir are :"+x);				
			}
		});
	    
	    textRDD.foreach(x->System.out.println("The elements read from TextFileDir are :"+x));
	    
	    //saveAsObjectFile
	    intRDD.saveAsObjectFile("ObjectFileDir");
	    JavaRDD<Integer> objectRDD= sparkContext.objectFile("ObjectFileDir");
	    objectRDD.foreach(new VoidFunction<Integer>() {
		@Override
			public void call(Integer x) throws Exception {
				System.out.println("The elements read from ObjectFileDir are :"+x);				
			}
		} );
	    
	    objectRDD.foreach(x->System.out.println("The elements read from ObjectFileDir are :"+x));
	    
	    
	    
	    
	    sparkContext.close();

	}

	static class integerComparator implements Comparator<Integer>,Serializable{
	private static final long serialVersionUID = 1L;
		@Override
		public int compare(Integer a, Integer b) {
			return a.compareTo(b);
		} 
	}
	
	static class integerReverseComparator implements Comparator<Integer>,Serializable{
		private static final long serialVersionUID = 1L;
			@Override
			public int compare(Integer a, Integer b) {
				return b.compareTo(a);
			} 
		}
}
