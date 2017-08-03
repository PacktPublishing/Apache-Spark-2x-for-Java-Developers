package com.packt.sfjd.ch5;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;import org.apache.spark.sql.SparkSession;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;

public class CassandraExample {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf =new SparkConf().setMaster("local").setAppName("Cassandra Example");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		//conf.set("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse");
		
		JavaSparkContext jsc=new JavaSparkContext(conf);
		JavaRDD<Employee> cassandraTable = CassandraJavaUtil.javaFunctions(jsc).cassandraTable("my_keyspace", "emp",CassandraJavaUtil.mapRowTo(Employee.class));
		
	    JavaRDD<String> selectEmpDept = CassandraJavaUtil.javaFunctions(jsc).cassandraTable("my_keyspace", "emp",CassandraJavaUtil.mapColumnTo(String.class)).select("emp_dept","emp_name");
		
		 
	    cassandraTable.collect().forEach(System.out::println);
	    //selectEmpDept.collect().forEach(System.out::println);
		
	    
	    CassandraJavaUtil.javaFunctions(cassandraTable)
        .writerBuilder("my_keyspace", "emp1", CassandraJavaUtil.mapToRow(Employee.class)).saveToCassandra();
	    
		/*SQLContext sqlContext = new SQLContext(jsc);
		
		Map<String,String> map =new HashMap<>();
		map.put("table" , "emp");
		map.put("keyspace", "my_keyspace");
		
		Dataset<Row> df = sqlContext.read().format("org.apache.spark.sql.cassandra")
		  .options(map)
		  .load();
		
		df.show();*/
		
		
		
	}
	
	
	
}
