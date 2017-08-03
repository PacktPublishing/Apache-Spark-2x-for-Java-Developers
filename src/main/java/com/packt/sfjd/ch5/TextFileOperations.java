package com.packt.sfjd.ch5;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TextFileOperations {

	public static void main(String[] args) {
		 System.setProperty("hadoop.home.dir", "E:\\sumitK\\Hadoop");
		 SparkConf conf = new SparkConf().setMaster("local").setAppName("ActionExamples").set("spark.hadoop.validateOutputSpecs", "false");
			JavaSparkContext sparkContext = new JavaSparkContext(conf);
			
		 JavaRDD<String> textFile = sparkContext.textFile("C:/Users/sumit.kumar/git/learning/src/main/resources/people.tsv");
				
				
				JavaRDD<Person> people =textFile.map(
				  line -> {
				  String[] parts = line.split("~");
				  Person person = new Person();
				  person.setName(parts[0]);
				  person.setAge(Integer.parseInt(parts[1].trim()));
				  person.setOccupation(parts[2]);
				  return person;
				});
				  
				  
				people.foreach(p -> System.out.println(p));
			
			
				
				JavaRDD<Person> peoplePart = textFile.mapPartitions(p -> {
					ArrayList<Person> personList=new ArrayList<Person>();
					while (p.hasNext()){
						String[] parts = p.next().split("~");
					      Person person = new Person();
					      person.setName(parts[0]);
					      person.setAge(Integer.parseInt(parts[1].trim()));	
					      person.setOccupation(parts[2]);
					      personList.add(person);
					}
					return  personList.iterator();
				});
				  
				peoplePart.foreach(p -> System.out.println(p));
			
				
				people.saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleSimple");
				people.repartition(1).saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleRepart");
				people.coalesce(1).saveAsTextFile("C:/Users/sumit.kumar/git/learning/src/main/resources/peopleCoalesce");
	}


	
}
