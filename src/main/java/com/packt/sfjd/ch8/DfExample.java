/*package com.packt.sfjd.ch8;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class DfExample {
	public static void main(String[] args) {

		
		 SparkConf conf =new SparkConf().setMaster("local").setAppName("Sql");

		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Employee> empRDD = jsc.parallelize(Arrays.asList(new Employee("Foo", 1),new Employee("Bar", 1)));
		SQLContext sqlContext = new SQLContext(jsc);
		
		DataFrame df = sqlContext.createDataFrame(empRDD, Employee.class);
		
		DataFrame filter = df.filter("id >1");
		
		filter.show();
		
	}
}
*/