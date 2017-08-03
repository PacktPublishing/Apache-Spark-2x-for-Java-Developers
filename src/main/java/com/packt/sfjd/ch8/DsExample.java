package com.packt.sfjd.ch8;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class DsExample {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\hadoop");
		SparkSession sparkSession = SparkSession.builder()
				.master("local")
				.appName("Spark Session Example")
				.config("spark.driver.memory", "2G")
				.config("spark.sql.warehouse.dir", "E:\\hadoop\\warehouse")
				.getOrCreate();
		
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		JavaRDD<Employee> empRDD = jsc.parallelize(Arrays.asList(new Employee( 1,"Foo"),new Employee( 2,"Bar")));
		
		//Dataset<Row> dataset = sparkSession.createDataFrame(empRDD, Employee.class);
		Dataset<Employee> dsEmp = sparkSession.createDataset(empRDD.rdd(), org.apache.spark.sql.Encoders.bean(Employee.class));
		Dataset<Employee> filter = dsEmp.filter(emp->emp.getEmpId()>1);
		//filter.show();
	
		
		Dataset<Row> dfEmp = sparkSession.createDataFrame(empRDD, Employee.class);
		dfEmp.show();
		
		Dataset<Row> filter2 = dfEmp.filter(row->row.getInt(2)> 1);
		filter2.show();
		
		//Three variants in which DataSet can be used 
		
		dsEmp.printSchema();
		
		//1. 
		dsEmp.filter(new FilterFunction<Employee>() {			
			@Override
			public boolean call(Employee emp) throws Exception {				
				return emp.getEmpId() > 1;
			}
		}).show();
		
		dsEmp.filter(emp -> emp.getEmpId()>1).show();
		
		//2.
		dsEmp.filter("empID > 1").show();
		
		//3. DSL
		dsEmp.filter(col("empId").gt(1)).show();
		
		
		
		
	}
}
