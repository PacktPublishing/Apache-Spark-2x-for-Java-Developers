package com.packt.sfjd.ch5;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFileOperations {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN); 
		      SparkSession sparkSession = SparkSession
		      .builder()
		      .master("local")
			  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
		      .appName("JavaALSExample")
		      .getOrCreate();
		      
		   RDD<String> textFile = sparkSession.sparkContext().textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json",2); 
		   
		   JavaRDD<PersonDetails> mapParser = textFile.toJavaRDD().map(v1 -> new ObjectMapper().readValue(v1, PersonDetails.class));
		   
		   mapParser.foreach(t -> System.out.println(t)); 
		  
		   Dataset<Row> anotherPeople = sparkSession.read().json(textFile);
		   
		   anotherPeople.printSchema();
		   anotherPeople.show();
		      
		      
		      Dataset<Row> json_rec = sparkSession.read().json("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_json.json");
		      json_rec.printSchema();
		      
		      json_rec.show();
		      
		      StructType schema = new StructType( new StructField[] {
		    	            DataTypes.createStructField("cid", DataTypes.IntegerType, true),
		    	            DataTypes.createStructField("county", DataTypes.StringType, true),
		    	            DataTypes.createStructField("firstName", DataTypes.StringType, true),
		    	            DataTypes.createStructField("sex", DataTypes.StringType, true),
		    	            DataTypes.createStructField("year", DataTypes.StringType, true),
		    	            DataTypes.createStructField("dateOfBirth", DataTypes.TimestampType, true) });
		      
		    /*  StructType pep = new StructType(new StructField[] {
						new StructField("Count", DataTypes.StringType, true, Metadata.empty()),
						new StructField("County", DataTypes.StringType, true, Metadata.empty()),
						new StructField("First Name", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Sex", DataTypes.StringType, true, Metadata.empty()),
						new StructField("Year", DataTypes.StringType, true, Metadata.empty()),
					    new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()) });*/
		      
		     Dataset<Row> person_mod = sparkSession.read().schema(schema).json(textFile);
		     
		     person_mod.printSchema();
		     person_mod.show();
		     
		     person_mod.write().format("json").mode("overwrite").save("C:/Users/sumit.kumar/git/learning/src/main/resources/pep_out.json");

	}

}
