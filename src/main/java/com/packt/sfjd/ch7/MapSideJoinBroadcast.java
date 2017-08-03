package com.packt.sfjd.ch7;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

public class MapSideJoinBroadcast {

	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").appName("My App")
				.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaPairRDD<String, String> userIdToCityId = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<String, String>("1", "101"), new Tuple2<String, String>("2", "102"),
						new Tuple2<String, String>("3", "107"), new Tuple2<String, String>("4", "103"),
						new Tuple2<String, String>("11", "101"), new Tuple2<String, String>("12", "102"),
						new Tuple2<String, String>("13", "107"), new Tuple2<String, String>("14", "103")));

		JavaPairRDD<String, String> cityIdToCityName = jsc.parallelizePairs(
				Arrays.asList(new Tuple2<String, String>("101", "India"), new Tuple2<String, String>("102", "UK"),
						new Tuple2<String, String>("103", "Germany"), new Tuple2<String, String>("107", "USA")));

		Broadcast<Map<String, String>> citiesBroadcasted = jsc.broadcast(cityIdToCityName.collectAsMap());

		JavaRDD<Tuple3<String, String, String>> joined = userIdToCityId.map(
				v1 -> new Tuple3<String, String, String>(v1._1(), v1._2(), citiesBroadcasted.value().get(v1._2())));

		System.out.println(joined.collect());

	}

}
