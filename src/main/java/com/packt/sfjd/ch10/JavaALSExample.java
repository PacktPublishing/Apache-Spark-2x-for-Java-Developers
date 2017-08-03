package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;


//examples/src/main/java/org/apache/spark/examples/ml/JavaALSExample.java
//  examples/src/main/scala/org/apache/spark/examples/ml/ALSExample.scala

public class JavaALSExample {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN); 
		      SparkSession spark = SparkSession
		      .builder()
		      .master("local")
			  .config("spark.sql.warehouse.dir","file:///E:/sumitK/Hadoop/warehouse")
		      .appName("JavaALSExample")
		      .getOrCreate();

		    // $example on$
		    JavaRDD<Rating> ratingsRDD = spark
		      .read().textFile("E:\\sumitK\\Hadoop\\movieLens-latest-small\\ratings.csv").javaRDD().filter(str-> !str.contains("userId"))
		      .map(new Function<String, Rating>() {
		        public Rating call(String str) {		        	
		          return Rating.parseRating(str);
		        }
		      });
		  
		  /*  Dataset<Row> ratingDS = spark.read()
		    		.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
		    		.option("header", "true")
			        .load("E:\\sumitK\\Hadoop\\movieLens-latest-small\\ratings.csv");*/
		    
		    
		    
		   Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
		   ratings.show();
		    Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
		    Dataset<Row> training = splits[0];
		    Dataset<Row> test = splits[1];
            System.out.println("The no of training rows are :"+training.count()+" and the row count of test are :"+test.count()); 
		    		    
		    // Build the recommendation model using ALS on the training data
		    ALS als = new ALS()
		      .setMaxIter(5)
		      .setRegParam(0.01)
		      .setUserCol("userId")
		      .setItemCol("movieId")
		      .setRatingCol("rating");
		    ALSModel model = als.fit(training);

		    // Evaluate the model by computing the RMSE on the test data
		    Dataset<Row> predictions = model.transform(test);
		    predictions.show();

		    RegressionEvaluator evaluator = new RegressionEvaluator()
		      .setMetricName("rmse")
		      .setLabelCol("rating")
		      .setPredictionCol("prediction");
		    Double rmse = evaluator.evaluate(predictions);
		    System.out.println("Root-mean-square error = " + rmse);
		    // $example off$
		    spark.stop();
		  }

}
