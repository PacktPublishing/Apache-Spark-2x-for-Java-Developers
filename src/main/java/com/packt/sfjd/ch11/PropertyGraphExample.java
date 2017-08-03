package com.packt.sfjd.ch11;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.GraphOps;
import org.apache.spark.graphx.PartitionStrategy;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.reflect.ClassTag;

public class PropertyGraphExample {
	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "C:\\softwares\\Winutils");
		SparkConf conf = new SparkConf().setMaster("local").setAppName("graph");
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		ClassTag<String> stringTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
		ClassTag<Integer> intTag = scala.reflect.ClassTag$.MODULE$.apply(Integer.class);
		
		
		
	 //$eq$colon$eq<Object, Object> scala$Predef$$singleton_$eq$colon$eq =  scala.Predef$.MODULE$.scala$Predef$$singleton_$eq$colon$eq;
   $eq$colon$eq<String, String> tpEquals = scala.Predef.$eq$colon$eq$.MODULE$.tpEquals();
		List<Tuple2<Object, String>> vertices = new ArrayList<>();

		vertices.add(new Tuple2<Object, String>(1l, "James"));
		vertices.add(new Tuple2<Object, String>(2l, "Robert"));
		vertices.add(new Tuple2<Object, String>(3l, "Charlie"));
		vertices.add(new Tuple2<Object, String>(4l, "Roger"));
		vertices.add(new Tuple2<Object, String>(5l, "Tony"));

		List<Edge<String>> edges = new ArrayList<>();

		edges.add(new Edge<String>(2, 1, "Friend"));
       edges.add(new Edge<String>(3, 2, "Advisor"));
		edges.add(new Edge<String>(3, 1, "Friend"));
		/*edges.add(new Edge<String>(1, 2, "Friend"));
	       edges.add(new Edge<String>(2, 3, "Advisor"));
			edges.add(new Edge<String>(1, 3, "Friend"));*/
		edges.add(new Edge<String>(4, 3, "colleague"));
		edges.add(new Edge<String>(4, 5, "Relative"));
		edges.add(new Edge<String>(5, 2, "BusinessPartners"));

		JavaRDD<Tuple2<Object, String>> verticesRDD = javaSparkContext.parallelize(vertices);
		JavaRDD<Edge<String>> edgesRDD = javaSparkContext.parallelize(edges);

		Graph<String, String> graph = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), "", StorageLevel.MEMORY_ONLY(),
				StorageLevel.MEMORY_ONLY(), stringTag, stringTag);

		
		
		
		graph.vertices().toJavaRDD().collect().forEach(System.out::println);
		/*System.out.println("-------------------------------");
		graph.edges().toJavaRDD().collect().forEach(System.out::println);*/
		
		//Graph operations
		
		//mapvertices
		
		/*Graph<String, String> mapVertices = graph.mapVertices(new AbsFunc3(), stringTag, tpEquals);
		mapVertices.vertices().toJavaRDD().collect().forEach(System.out::println);*/
		
		//mapEdges
		
		/*Graph<String, Integer> mapEdges = graph.mapEdges(new AbsFunc7(), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
		mapEdges.edges().toJavaRDD().collect().forEach(System.out::println);*/
		
		//mapTriplets
		//Graph<String, Integer> mapTriplets = graph.mapTriplets(new AbsFunc8(), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
		//mapTriplets.triplets().toJavaRDD().collect().forEach(System.out::println);
		
		 //Other way - loose indices
		//JavaRDD<String> map = graph.vertices().toJavaRDD().map(x->"Vertex:"+x);
		
		//Triplets
		
		//Reverse
		/* Graph<String, String> reversedGraph = graph.reverse();
		 reversedGraph.triplets().toJavaRDD().collect().forEach(System.out::println);*/
		

		//Subgraph
	/*	Graph<String, String> subgraph = graph.subgraph(new AbsFunc1(), new AbsFunc2());
		subgraph.triplets().toJavaRDD().collect().forEach(System.out::println);*/
		
		//Aggregate Messages
			
		/*VertexRDD<Integer> aggregateMessages = graph.aggregateMessages(new AbsFunc4(), new AbsFunc5(), TripletFields.All, intTag);
		
		aggregateMessages.toJavaRDD().collect().forEach(System.out::println);*/
		
		
		
		//Join 
//		List<Tuple2<Object, String>> dataToJoin = new ArrayList<>();
//
//		dataToJoin.add(new Tuple2<Object, String>(1l,"Wilson"));
//		dataToJoin.add(new Tuple2<Object, String>(2l,"Harmon"));
//		dataToJoin.add(new Tuple2<Object, String>(3l,"Johnson"));
//		dataToJoin.add(new Tuple2<Object, String>(4l,"Peterson"));
//		dataToJoin.add(new Tuple2<Object, String>(5l,"Adams"));
//		
//		JavaRDD<Tuple2<Object, String>> dataToJoinRdd = javaSparkContext.parallelize(dataToJoin);
//		
//		Graph<String, String> outerJoinVertices = graph.outerJoinVertices(dataToJoinRdd.rdd(), new AbsFunc6(), scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.Predef.$eq$colon$eq$.MODULE$.tpEquals());
//		outerJoinVertices.vertices().toJavaRDD().collect().forEach(System.out::println);
	
	
		//Graph-Anaytics
		
		//PageRank	
		/*Graph<Object, Object> graphWithStaticRanking = graph.ops().staticPageRank(1,0.20);
		graphWithStaticRanking.vertices().toJavaRDD().collect().forEach(System.out::println);
		*/
		//graph.ops().pageRank(0.00001,0.20).vertices().toJavaRDD().collect().forEach(System.out::println);;
		
		//Triangle count
		graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut$.MODULE$);
		
		Graph<Object, String> triangleCountedGraph = graph.ops().triangleCount();
		triangleCountedGraph.vertices().toJavaRDD().collect().forEach(System.out::println);
		
		//Connected components
		/*Graph<Object, String> connectedComponentsGraph = graph.ops().connectedComponents();
		connectedComponentsGraph.vertices().toJavaRDD().collect().forEach(System.out::println);;*/
		
		/*scala.collection.immutable.Set<Object> set = new scala.collection.immutable.HashSet<Object>();
		List<String> list =new ArrayList<>();
		 
		JavaConverters.collectionAsScalaIterableConverter(list).asScala().toSeq();*/
//	ShortestPaths.run
		
		
		
	}

}
