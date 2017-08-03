package com.packt.sfjd.ch5;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class S3Example {

	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf =new SparkConf().setMaster("local").setAppName("S3 Example");
		JavaSparkContext jsc=new JavaSparkContext(conf);
		//jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "Your awsAccessKeyId");
		//jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "your awsSecretAccessKey");
		
		
		System.out.println(System.getenv("AWS_ACCESS_KEY_ID"));
		JavaRDD<String> textFile = jsc.textFile("s3a://"+"trust"+"/"+"MOCK_DATA.csv");
		
//		textFile.flatMap(x -> Arrays.asList(x.split(",")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
//		.reduceByKey((x, y) -> x + y).saveAsTextFile("s3n://"+"trust"+"/"+"out.txt");
		
		textFile.flatMap(x -> Arrays.asList(x.split(",")).iterator()).mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
		.reduceByKey((x, y) -> x + y).saveAsTextFile("s3a://"+"trust"+"/"+"out.txt");
	}
}
