package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.types.DataTypes;


import static org.apache.spark.sql.functions.col;

//https://docs.cloud.databricks.com/docs/latest/sample_applications/index.html#Sample%20ML/MLPipeline%20Bike%20Dataset.html

public class BikeRentalPrediction {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		SparkSession sparkSession = SparkSession
				.builder()
				.master("local")
				.config("spark.sql.warehouse.dir",
						"file:///E:/sumitK/Hadoop/warehouse")
				.appName("BikeRentalPrediction").getOrCreate();
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		//We use the sqlContext.read method to read the data and set a few options:
		//  'format': specifies the Spark CSV data source
		//  'header': set to true to indicate that the first line of the CSV data file is a header
	    // The file is called 'hour.csv'.	
		Dataset<Row> ds=sparkSession.read()
				  .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
				  .option("header", "true")
				  .load("E:\\sumitK\\Hadoop\\Bike-Sharing-Dataset\\hour.csv");
		
		ds.cache();
		
		ds.select("season").show();;
		
		ds.show();
		
		System.out.println("Our dataset has rows :: "+ ds.count());
		
		Dataset<Row> df = ds.drop("instant").drop("dteday").drop("casual").drop("registered");
		df.printSchema();
		//col("...") is preferable to df.col("...")
		Dataset<Row> dformatted = df.select(col("season").cast(DataTypes.IntegerType),
				                            col("yr").cast(DataTypes.IntegerType),
											col("mnth").cast(DataTypes.IntegerType),
											col("hr").cast(DataTypes.IntegerType),
											col("holiday").cast(DataTypes.IntegerType),
											col("weekday").cast(DataTypes.IntegerType),
											col("workingday").cast(DataTypes.IntegerType),
											col("weathersit").cast(DataTypes.IntegerType),
											col("temp").cast(DataTypes.IntegerType),
											col("atemp").cast(DataTypes.IntegerType),
											col("hum").cast(DataTypes.IntegerType),
											col("windspeed").cast(DataTypes.IntegerType),
											col("cnt").cast(DataTypes.IntegerType));
		
		
	dformatted.printSchema();	
	Dataset<Row>[] data=	dformatted.randomSplit(new double[]{0.7,0.3});
	System.out.println("We have training examples count :: "+ data[0].count()+" and test examples count ::"+data[1].count());
	
	///
	//removing 'cnt' cloumn and then forming str array
	String[] featuresCols = dformatted.drop("cnt").columns();
	
	for(String str:featuresCols){
		System.out.println(str+" :: ");
	}
	
	//This concatenates all feature columns into a single feature vector in a new column "rawFeatures".
	VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("rawFeatures");
	//This identifies categorical features and indexes them.
	VectorIndexer vectorIndexer= new VectorIndexer().setInputCol("rawFeatures").setOutputCol("features").setMaxCategories(4);

	//Takes the "features" column and learns to predict "cnt"
	GBTRegressor gbt = new GBTRegressor().setLabelCol("cnt");
			
	// Define a grid of hyperparameters to test:
   //  - maxDepth: max depth of each decision tree in the GBT ensemble
	//  - maxIter: iterations, i.e., number of trees in each GBT ensemble
	// In this example notebook, we keep these values small.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100).
	ParamMap[]	paramGrid = new ParamGridBuilder().addGrid(gbt.maxDepth(),new int[]{2, 5}).addGrid(gbt.maxIter(),new int[] {10, 100}).build();
	// We define an evaluation metric.  This tells CrossValidator how well we are doing by comparing the true labels with predictions.
	RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol(gbt.getLabelCol()).setPredictionCol(gbt.getPredictionCol());
	
	//	# Declare the CrossValidator, which runs model tuning for us.
		CrossValidator cv = new CrossValidator().setEstimator(gbt).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid);
			
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{vectorAssembler,vectorIndexer,cv});
				
		PipelineModel pipelineModel=pipeline.fit(data[0]);
		
		Dataset<Row> predictions = pipelineModel.transform(data[1]);
		
		predictions.show();
		//predictions.select("cnt", "prediction", *featuresCols);
	}

}


