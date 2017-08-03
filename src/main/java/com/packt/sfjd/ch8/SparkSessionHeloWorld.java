package com.packt.sfjd.ch8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionHeloWorld {
public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
	SparkSession sparkSession = SparkSession.builder()
			.master("local")
			.appName("CSV Read Example")
			.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse")
			.getOrCreate();
	
	Dataset<Row> csv = sparkSession.read().format("com.databricks.spark.csv").option("header","true")
			.load("C:\\Users\\sgulati\\Documents\\my_docs\\book\\testdata\\emp.csv");
	
	csv.createOrReplaceTempView("test");
	Dataset<Row> sql = sparkSession.sql("select * from test");
	sql.collectAsList();
}
}
