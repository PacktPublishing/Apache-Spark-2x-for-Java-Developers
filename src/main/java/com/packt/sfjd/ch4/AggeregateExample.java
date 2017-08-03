package com.packt.sfjd.ch4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class AggeregateExample {

	public static void main(String[] args) {
		 String master;
		    if (args.length > 0) {
			    master = args[0];
		    } else {
			    master = "local";
		    }

		    JavaSparkContext sc = new JavaSparkContext(
		      master, "AggeregateExample");
		    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3,4,5),3);		    
		    System.out.println("The no of partitions are ::"+rdd.getNumPartitions());
		    //TODO print elements with partition with index mappationwithindex()
		    Function2<String,Integer,String> agg=new Function2<String, Integer, String>() {
				@Override
				public String call(String v1, Integer v2) throws Exception {
				return v1+v2;
				}
			} ;
			
			Function2<String,String,String> combineAgg=new Function2<String, String, String>() {
				@Override
				public String call(String v1, String v2) throws Exception {
					return v1+v2;
				}
			};
			
			
			//String result= rdd.aggregate("X", agg, combineAgg); 
		   String result= rdd.aggregate("X", (x,y)->x+y, (x,z)->x+z);
		   System.out.println("The aggerate value is ::"+result);
		   
		  
		  int res= rdd.aggregate(3, (x,y)-> x>y?x:y, (w,z)->w>z?w:z);
            System.out.println("the res is ::"+res);
            
            List<Tuple2<String,Integer>> listS = new ArrayList<Tuple2<String,Integer>>();
            listS.add(new Tuple2<String,Integer>("a", 1));
            listS.add(new Tuple2<String,Integer>("b", 2));
            listS.add(new Tuple2<String,Integer>("c", 3));
            listS.add(new Tuple2<String,Integer>("a", 4));

            // 
            JavaPairRDD<String,Integer> R = sc.parallelizePairs(listS);
            List<Tuple2<String,Integer>> es= R.aggregateByKey(1, (x,y)->x+y, (x,y)->x+y).collect();
            
            for (Tuple2<String, Integer> tuple2 : es) {
				System.out.println("the key is"+tuple2._1()+" and the val is ::"+tuple2._2());
			}
            
	}

}
