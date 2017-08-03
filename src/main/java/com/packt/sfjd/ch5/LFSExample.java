package com.packt.sfjd.ch5;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LFSExample {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf =new SparkConf().setMaster("local").setAppName("Local File System Example");
		
		
		JavaSparkContext jsc=new JavaSparkContext(conf);
	//	jsc.hadoopConfiguration().setLong("dfs.block.size",20000);
	  
	//	jsc.hadoopConfiguration().setLong("fs.local.block.size",20000);
		  
//		
//		JavaRDD<String> localFile=jsc.textFile("C:\\Users\\sgulati\\Documents\\Result\\test\\a.txt");
//		localFile.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//		.reduceByKey((x, y) -> x + y).saveAsTextFile("C:\\Users\\sgulati\\Documents\\Result\\out_path");
		
		
//		JavaRDD<String> localFile1 = jsc.textFile("C:\\Users\\sgulati\\Documents\\Result\\test\\a.txt,C:\\Users\\sgulati\\Documents\\Result\\test\\b.txt");
//		
//		System.out.println(localFile1.getNumPartitions());
//		localFile1.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//		.reduceByKey((x, y) -> x + y).saveAsTextFile("C:\\Users\\sgulati\\Documents\\Result\\out_path1");
		
		JavaRDD<String> localFile2 =jsc.textFile("C:\\Users\\sgulati\\Documents\\Result\\test\\*");
		System.out.println(localFile2.getNumPartitions());
//		localFile2.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//		.reduceByKey((x, y) -> x + y).saveAsTextFile("C:\\Users\\sgulati\\Documents\\Result\\out_path2");
////	   
//        JavaRDD<String> localFile3 =jsc.textFile("C:\\Users\\sgulati\\Documents\\Result\\test\\*,C:\\Users\\sgulati\\Documents\\Result\\test5\\*");
//		
//        localFile3.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//		.reduceByKey((x, y) -> x + y).saveAsTextFile("C:\\Users\\sgulati\\Documents\\Result\\out_path3");
//        
//        JavaPairRDD<String, String> localFileWhole = jsc.wholeTextFiles("C:\\Users\\sgulati\\Documents\\Result\\test\\a.txt,C:\\Users\\sgulati\\Documents\\Result\\test\\b.txt");
//        System.out.println(localFileWhole.collect());
        
		jsc.close();
		
	}
}
