package com.packt.sfjd.ch7;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import net.sf.saxon.expr.flwor.Tuple;
import scala.Tuple2;

public class Transformations {
	public static void main(String[] args) {

		SparkSession sparkSession = SparkSession.builder().master("local").appName("My App")
				.config("spark.sql.warehouse.dir", "file:////C:/Users/sgulati/spark-warehouse").getOrCreate();

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		JavaRDD<Integer> intRDD = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2);


		JavaRDD<Integer> mapPartitions = intRDD.mapPartitions(iterator -> {
			List<Integer> intList = new ArrayList<>();
			while (iterator.hasNext()) {
				intList.add(iterator.next() + 1);
			}

			return intList.iterator();
		});

		intRDD.mapPartitionsWithIndex((index, iterator) -> {
			List<String> list = new ArrayList<String>();
			while (iterator.hasNext()) {
				list.add("Element " + iterator.next() + " belongs to partition " + index);
			}
			return list.iterator();
		}, false);

		JavaPairRDD<String, Integer> pairRDD = intRDD.mapPartitionsToPair(t -> {
			List<Tuple2<String, Integer>> list = new ArrayList<>();
			while (t.hasNext()) {
				int element = t.next();
				list.add(element % 2 == 0 ? new Tuple2<String, Integer>("even", element)
						: new Tuple2<String, Integer>("odd", element));
			}
			return list.iterator();
		});
		JavaPairRDD<String, Integer> mapValues = pairRDD.mapValues(v1 -> v1 * 3);

		// System.out.println(mapValues.collect());

		// intRDD.mapPartitionsToPair(f)
		/*
		 * intRDD.mapPartitionsWithIndex(new Function2<Integer,
		 * Iterator<Integer>, Iterator<Integer>>() {
		 * 
		 * @Override public Iterator<Integer> call(Integer v1, Iterator<Integer>
		 * v2) throws Exception { // TODO Auto-generated method stub return
		 * null; } }, true);
		 */
		// System.out.println(mapPartitions.toDebugString());

		// sort bykey
		JavaPairRDD<String, String> monExpRDD = jsc
				.parallelizePairs(Arrays.asList(new Tuple2<String, String>("Jan", "50,100,214,10"),
						new Tuple2<String, String>("Feb", "60,314,223,77")));

		JavaPairRDD<String, Integer> monExpflattened = monExpRDD
				.flatMapValues(new Function<String, Iterable<Integer>>() {
					@Override
					public Iterable<Integer> call(String v1) throws Exception {
						List<Integer> list = new ArrayList<>();

						String[] split = v1.split(",");

						for (String s : split) {
							list.add(Integer.parseInt(s));
						}
						return list;
					}
				});

		JavaPairRDD<String, Integer> monExpflattened1 = monExpRDD.flatMapValues(
				v -> Arrays.asList(v.split(",")).stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList()));

		JavaPairRDD<String, Integer> repartitionAndSortWithinPartitions = monExpflattened
				.repartitionAndSortWithinPartitions(new HashPartitioner(2));
		JavaPairRDD<Integer, String> unPartitionedRDD = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer, String>(8, "h"),
				new Tuple2<Integer, String>(5, "e"), new Tuple2<Integer, String>(4, "d"),
				new Tuple2<Integer, String>(2, "a"), new Tuple2<Integer, String>(7, "g"),
				new Tuple2<Integer, String>(6, "f"),new Tuple2<Integer, String>(1, "a"),
				new Tuple2<Integer, String>(3, "c"),new Tuple2<Integer, String>(3, "z")));
		
		
		JavaPairRDD<Integer, String> repartitionAndSortWithinPartitions2 = unPartitionedRDD.repartitionAndSortWithinPartitions(new HashPartitioner(3));
		
		
		pairRDD.coalesce(2);
		
		
		
		
		JavaPairRDD<String, String> pairRDD3 = jsc.parallelizePairs(Arrays.asList(
				new Tuple2<String, String>("key1", "Austria"), new Tuple2<String, String>("key2", "Australia"),
				new Tuple2<String, String>("key3", "Antartica"), new Tuple2<String, String>("key1", "Asia"),
				new Tuple2<String, String>("key2", "France"),new Tuple2<String, String>("key3", "Canada"),
				new Tuple2<String, String>("key1", "Argentina"),new Tuple2<String, String>("key2", "American Samoa"),
				new Tuple2<String, String>("key3", "Germany")),1);
	//	System.out.println(pairRDD3.getNumPartitions());
		
		JavaPairRDD<String, Integer> aggregateByKey = pairRDD3.aggregateByKey(0, (v1, v2) -> {
			System.out.println(v2);
			if(v2.startsWith("A")){
				v1+=1;
			}
			
			return v1;
		}, (v1, v2) -> v1+v2);
		
		
		JavaPairRDD<String, Integer> combineByKey = pairRDD3.combineByKey(v1 -> {
			if(v1.startsWith("A")){
				return 1;
			}
			else{
				return 0;
			}
		}, (v1, v2) -> {
			
			if(v2.startsWith("A")){
				v1+=1;
			}
			
			return v1;
		}, (v1, v2) -> v1+v2);
		
		
		JavaRDD<String> stringRDD = jsc.parallelize(Arrays.asList("Hello Spark", "Hello Java"));
		JavaPairRDD<String, Integer> flatMapToPair = stringRDD.flatMapToPair(s -> Arrays.asList(s.split(" ")).stream()
				.map(token -> new Tuple2<String, Integer>(token, 1)).collect(Collectors.toList())
				.iterator());
		flatMapToPair.foldByKey(0,(v1, v2) -> v1+v2).collect();
		

	}
}
