package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;

import scala.Function1;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction1;

public class PropertyGraphExampleFromEdges {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);


		List<Edge<String>> edges = new ArrayList<>();

		edges.add(new Edge<String>(1, 2, "Friend"));
		edges.add(new Edge<String>(2, 3, "Advisor"));
		edges.add(new Edge<String>(1, 3, "Friend"));
		edges.add(new Edge<String>(4, 3, "colleague"));
		edges.add(new Edge<String>(4, 5, "Relative"));
		edges.add(new Edge<String>(2, 5, "BusinessPartners"));


		JavaRDD<Edge<String>> edgeRDD = javaSparkContext.parallelize(edges);
		
		
		Graph<String, String> graph = Graph.fromEdges(edgeRDD.rdd(), "",StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), stringTag, stringTag);
		
		
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		
		
//	graph.aggregateMessages(sendMsg, mergeMsg, tripletFields, evidence$11)	
		
	}
}
