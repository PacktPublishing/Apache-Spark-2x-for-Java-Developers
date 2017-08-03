package com.packt.sfjd.ch4.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class Transformations {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("ApacheSparkForJavaDevelopers");
		// SparkContext context =new SparkContext(conf);
		// RDD<String> textFile = context.textFile("abc", 1);

		JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

		List<Integer> intList = Arrays.asList(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(intList, 2);
		// intRDD.repartition(2);

		// Map Transformation
		JavaRDD<Integer> mappedRDD = intRDD.map(x -> x + 1);

		// Map with partitions
		JavaRDD<Integer> mapPartitions = intRDD.mapPartitions(iterator -> {
			int sum = 0;
			while (iterator.hasNext()) {
				sum += iterator.next();
			}
			return Arrays.asList(sum).iterator();
		});

		// map partitions with index
		JavaRDD<String> mapPartitionsWithIndex = intRDD
				.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 739746028261776589L;

					@Override
					public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
						int sum = 0;
						while (iterator.hasNext()) {
							sum += iterator.next();
						}
						return Arrays.asList(index + ":" + sum).iterator();
					}
				}, true);

		// filter RDD
		JavaRDD<Integer> filter = intRDD.filter(x -> (x % 2 == 0));

		JavaRDD<String> stringRDD = javaSparkContext.parallelize(Arrays.asList("Hello Spark", "Hello Java"));

		// flat map

		JavaRDD<String> flatMap = stringRDD.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		// map to pair

		JavaPairRDD<String, Integer> pairRDD = intRDD.mapToPair(
				i -> (i % 2 == 0) ? new Tuple2<String, Integer>("even", i) : new Tuple2<String, Integer>("odd", i));

		// flat map to pair

		JavaPairRDD<String, Integer> flatMapToPair = stringRDD.flatMapToPair(s -> Arrays.asList(s.split(" ")).stream()
				.map(token -> new Tuple2<String, Integer>(token, token.length())).collect(Collectors.toList())
				.iterator());
		// List<Tuple2<String,Integer>> list =new ArrayList<>();
		// for (String token : s.split(" ")) {
		// list.add(new Tuple2<String, Integer>(token, token.length()));
		//
		// }
		// return list.iterator();

		// sample
		JavaRDD<Integer> sample = intRDD.sample(true, 2);

		// union
		JavaRDD<Integer> intRDD2 = javaSparkContext.parallelize(Arrays.asList(1, 2, 3));
		JavaRDD<Integer> union = intRDD.union(intRDD2);

		// intersection
		JavaRDD<Integer> intersection = intRDD.intersection(intRDD2);
		JavaRDD<Integer> subtract = intRDD.subtract(intRDD2);

		// distinct
		JavaRDD<Integer> rddwithdupElements = javaSparkContext
				.parallelize(Arrays.asList(1, 1, 2, 4, 5, 6, 8, 8, 9, 10, 11, 11));
		JavaRDD<Integer> distinct = rddwithdupElements.distinct();

		pairRDD.repartition(2);

		// groupbykey
		JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();

		// reducebykey
		JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});

		// sort by key
		JavaPairRDD<String, Integer> sortByKey = pairRDD.sortByKey();

		JavaPairRDD<String, Integer> aggregateByKey = pairRDD.aggregateByKey(0,
				new Function2<Integer, Integer, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -9193256894160862119L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}, new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {

						return v1 + v2;
					}
				});

		JavaPairRDD<String, Integer> combineByKey = pairRDD.combineByKey(new Function<Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -1965754276530922495L;

			@Override
			public Integer call(Integer v1) throws Exception {

				return v1;
			}
		}, new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -9193256894160862119L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}, new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {

				return v1 + v2;
			}
		});

		JavaPairRDD<String, Integer> foldByKey = pairRDD.foldByKey(0, new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {

				return v1 + v2;
			}
		});

		
		// System.out.println(intRDD.collect());
		// System.out.println(intRDD2.collect());

JavaRDD<String> rddStrings = javaSparkContext.parallelize(Arrays.asList("A","B","C"));
JavaRDD<Integer> rddIntegers = javaSparkContext.parallelize(Arrays.asList(1,4,5));

rddStrings.cartesian(rddIntegers);


//sort bykey
JavaPairRDD<String, Integer> unsortedPairRDD = javaSparkContext.parallelizePairs(
		Arrays.asList(new Tuple2<String, Integer>("B", 2), new Tuple2<String, Integer>("C", 5),
				new Tuple2<String, Integer>("D", 7), new Tuple2<String, Integer>("A", 8)));

unsortedPairRDD.sortByKey(false);

		
		
		// join
		
        
		JavaPairRDD<String, String> pairRDD1 = javaSparkContext.parallelizePairs(
				Arrays.asList(new Tuple2<String, String>("B", "A"), new Tuple2<String, String>("C", "D"),
						new Tuple2<String, String>("D", "E"), new Tuple2<String, String>("A", "B")));
		JavaPairRDD<String, Integer> pairRDD2 = javaSparkContext.parallelizePairs(
				Arrays.asList(new Tuple2<String, Integer>("B", 2), new Tuple2<String, Integer>("C", 5),
						new Tuple2<String, Integer>("D", 7), new Tuple2<String, Integer>("A", 8)));
		JavaPairRDD<String, Tuple2<String, Integer>> joinedRDD = pairRDD1.join(pairRDD2);
		pairRDD1.leftOuterJoin(pairRDD2);
		pairRDD1.rightOuterJoin(pairRDD2);
		pairRDD1.fullOuterJoin(pairRDD2);
		System.out.println(joinedRDD.collect());
		
		//cogroup
		JavaPairRDD<String, String> pairRDD3 = javaSparkContext.parallelizePairs(
				Arrays.asList(new Tuple2<String, String>("B", "A"), new Tuple2<String, String>("B", "D"),
						new Tuple2<String, String>("A", "E"), new Tuple2<String, String>("A", "B")));
		JavaPairRDD<String, Integer> pairRDD4 = javaSparkContext.parallelizePairs(
				Arrays.asList(new Tuple2<String, Integer>("B", 2), new Tuple2<String, Integer>("B", 5),
						new Tuple2<String, Integer>("A", 7), new Tuple2<String, Integer>("A", 8)));
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = pairRDD3.cogroup(pairRDD4);
			JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> groupWith = pairRDD3.groupWith(pairRDD4);
		
		System.out.println(cogroup.collect());
		
		
		
	}
}
