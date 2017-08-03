package com.packt.sfjd.ch10;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.ml.feature.Bucketizer;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorSlicer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

//https://community.hortonworks.com/articles/53903/spark-machine-learning-pipeline-by-example.html
//http://stackoverflow.com/questions/35844330/vectorassembler-output-only-to-densevector
//http://stackoverflow.com/questions/35224675/spark-ml-stringindexer-throws-unseen-label-on-fit
public class FlightDelay {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		SparkSession sparkSession = SparkSession
				.builder()
				.master("local")
				.config("spark.sql.warehouse.dir",
						"file:///E:/sumitK/Hadoop/warehouse")
				.appName("FlightDelay").getOrCreate();
		Logger rootLogger = LogManager.getRootLogger();
		rootLogger.setLevel(Level.WARN);
		
		
		//sparkSession.read().op.load(path)
		//
		Dataset<String> flight2007 = sparkSession.read()
				.textFile("E:\\sumitK\\Hadoop\\flights_2007.csv.bz2")
				.limit(999999);
		Dataset<String> flight2008 = sparkSession.read()
				.textFile("E:\\sumitK\\Hadoop\\flights_2008.csv.bz2")
				.limit(999999);

		String header = flight2007.head();
		System.out.println("The header in the file is :: " + header);
		// val trainingData = flight2007
		// .filter(x => x != header)
		// .map(x => x.split(","))
		// .filter(x => x(21) == "0")
		// .filter(x => x(17) == "ORD")
		// .filter(x => x(14) != "NA")
		// .map(p => Flight(p(1), p(2), p(3), getMinuteOfDay(p(4)),
		// getMinuteOfDay(p(5)), getMinuteOfDay(p(6)), getMinuteOfDay(p(7)),
		// p(8), p(11).toInt, p(12).toInt, p(13).toInt, p(14).toDouble,
		// p(15).toInt, p(16), p(18).toInt))
		// .toDF
		// / header.s
		// p-> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
		// getMinuteOfDay(p[5]), getMinuteOfDay(p[6]), getMinuteOfDay(p[7]),
		// p[8], (p[11].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[11]),
		// (p[12].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[12]),
		// (p[13].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[13]),
		// Double.parseDouble(p[14]),
		// (p[15].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[15]), p[16],
		// (p[18].equalsIgnoreCase("NA"))?0:Integer.parseInt(p[18])
		JavaRDD<Flight> trainingRDD = flight2007
				.filter(x -> !x.equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
						getMinuteOfDay(p[5]), getMinuteOfDay(p[6]),
						getMinuteOfDay(p[7]), p[8], (p[11]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[11]),
						(p[12].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[12]),
						(p[13].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[13]),
						(p[14].equalsIgnoreCase("NA")) ? 0 : Double
								.parseDouble(p[14]), (p[15]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[15]), p[16], (p[18]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[18])));
		Dataset<Row> trainingData = sparkSession.createDataFrame(trainingRDD,Flight.class).cache();

		// String header1 = flight2008.head();
		JavaRDD<Flight> testingRDD = flight2008
				.filter(x -> !x.equalsIgnoreCase(header))
				.javaRDD()
				.map(x -> x.split(","))
				.filter(x -> x[21].equalsIgnoreCase("0"))
				.filter(x -> x[17].equalsIgnoreCase("ORD"))
				.filter(x -> x[14].equalsIgnoreCase("NA"))
				.map(p -> new Flight(p[1], p[2], p[3], getMinuteOfDay(p[4]),
						getMinuteOfDay(p[5]), getMinuteOfDay(p[6]),
						getMinuteOfDay(p[7]), p[8], (p[11]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[11]),
						(p[12].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[12]),
						(p[13].equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[13]),
						(p[14].equalsIgnoreCase("NA")) ? 0 : Double
								.parseDouble(p[14]), (p[15]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[15]), p[16], (p[18]
								.equalsIgnoreCase("NA")) ? 0 : Integer
								.parseInt(p[18])));

		Dataset<Row> testingData = sparkSession.createDataFrame(testingRDD,	Flight.class).cache();

		System.out.println("The training data count is "+trainingData.count());

		System.out.println("The testing data count is "+testingData.count());
		
		
		//tranformor to convert string to category values
		StringIndexer monthIndexer = new StringIndexer().setInputCol("month").setOutputCol("MonthCat").setHandleInvalid("skip");
		StringIndexer dayofMonthIndexer = new StringIndexer().setInputCol("dayofMonth").setOutputCol("DayofMonthCat").setHandleInvalid("skip");
		StringIndexer dayOfWeekIndexer = new StringIndexer().setInputCol("dayOfWeek").setOutputCol("DayOfWeekCat").setHandleInvalid("skip");
		StringIndexer uniqueCarrierIndexer = new StringIndexer().setInputCol("uniqueCarrier").setOutputCol("UniqueCarrierCat").setHandleInvalid("skip");
		StringIndexer originIndexer = new StringIndexer().setInputCol("origin").setOutputCol("OriginCat").setHandleInvalid("skip");
		
		
		//assemble raw feature
		VectorAssembler assembler = new VectorAssembler()
		                    .setInputCols(new String[] {"MonthCat", "DayofMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "OriginCat", "depTime", "CRSDepTime", "arrTime", "CRSArrTime", "actualElapsedTime", "CRSElapsedTime", "airTime","depDelay", "distance"})
		                    .setOutputCol("rawFeatures");
		
		////////////////////////////////////////////////////////////////////////////////////////
		
		//vestor slicer
		VectorSlicer slicer = new VectorSlicer().setInputCol("rawFeatures").setOutputCol("slicedfeatures").setNames(new String[] {"MonthCat", "DayofMonthCat", "DayOfWeekCat", "UniqueCarrierCat", "depTime", "arrTime", "actualElapsedTime", "airTime", "depDelay", "distance"});
		//scale the features
		//Made a change by setting withMean as false which converts the output to dense vector
		//https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/feature/StandardScaler.scala
		StandardScaler scaler = new StandardScaler().setInputCol("slicedfeatures").setOutputCol("features").setWithStd(true).setWithMean(false);
		//labels for binary classifier
		Binarizer binarizerClassifier = new Binarizer().setInputCol("arrDelay").setOutputCol("binaryLabel").setThreshold(15.0);
		//logistic regression
		LogisticRegression lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol("binaryLabel").setFeaturesCol("features");
		// Chain indexers and tree in a Pipeline
		Pipeline lrPipeline = new Pipeline().setStages(new PipelineStage[] {monthIndexer, dayofMonthIndexer, dayOfWeekIndexer, uniqueCarrierIndexer, originIndexer, assembler, slicer, scaler, binarizerClassifier, lr});
		// Train model. 
		StructType str=trainingData.schema();
		//str.catalogString();
		//str.fieldNames();
		str.printTreeString();
		
		PipelineModel lrModel = lrPipeline.fit(trainingData);
		// Make predictions.
		Dataset<Row> lrPredictions = lrModel.transform(testingData);
		// Select example rows to display.
		lrPredictions.select("prediction", "binaryLabel", "features").show(20);
		
		

	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
		//index category index in raw feature
		VectorIndexer indexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("rawFeaturesIndexed").setMaxCategories(10);
		//PCA
		PCA pca = new PCA().setInputCol("rawFeaturesIndexed").setOutputCol("features").setK(10);
		//label for multi class classifier
		Bucketizer bucketizer = new Bucketizer().setInputCol("arrDelay").setOutputCol("multiClassLabel").setSplits(new double[]{Double.NEGATIVE_INFINITY, 0.0, 15.0, Double.POSITIVE_INFINITY});
		// Train a DecisionTree model.
		DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("multiClassLabel").setFeaturesCol("features");
		// Chain all into a Pipeline
		Pipeline dtPipeline = new Pipeline().setStages(new PipelineStage[] {monthIndexer, dayofMonthIndexer, dayOfWeekIndexer, uniqueCarrierIndexer, originIndexer, assembler, indexer, pca, bucketizer, dt});
		// Train model. 
		PipelineModel dtModel = dtPipeline.fit(trainingData);
		// Make predictions.
		Dataset<Row> dtPredictions = dtModel.transform(testingData);
		// Select example rows to display.
		dtPredictions.select("prediction", "multiClassLabel", "features").show(20);
		
		
	
		
		sparkSession.stop();

	}

	// calculate minuted from midnight, input is military time format
	private static Integer getMinuteOfDay(String depTime) {
		//System.out.println("the deptTime is:" + depTime);
		return (depTime.equalsIgnoreCase("NA")) ? 0
				: ((Integer.parseInt(depTime) / 100) * 60 + (Integer.parseInt(depTime) % 100));
	}

}
