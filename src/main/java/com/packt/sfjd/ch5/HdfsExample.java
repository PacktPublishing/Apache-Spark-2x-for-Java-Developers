package com.packt.sfjd.ch5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HdfsExample {
public static void main(String[] args) {
	
	SparkConf conf =new SparkConf().setMaster("local").setAppName("S3 Example");
	JavaSparkContext jsc=new JavaSparkContext(conf);
	jsc.hadoopConfiguration().setLong("dfs.blocksize",2);
	//jsc.hadoopConfiguration().setLong("fs.local.block.size",2);
	
	JavaRDD<String> hadoopRdd = jsc.textFile("hdfs://ch3lxesgdi02.corp.equinix.com:8020/user/gse/packt/ch01/test1",2);
	
	System.out.println(hadoopRdd.getNumPartitions());
	//hadoopRdd.saveAsTextFile("hdfs://ch3lxesgdi02.corp.equinix.com:8020/user/gse/packt/ch01/testout");
	
	
}
}
