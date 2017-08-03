package com.packt.sfjd.ch2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectorsExamples {

	public static void main(String[] args) {
		
		Supplier<Stream<String>> streamSupplier =()->Stream.of( new String[]{"The","Stream","from","an","array","of","The","Strings"} ) ;
		
		//String Concatenation using non parameterized joining
		String concatedString = streamSupplier.get().collect(Collectors.joining());
		System.out.println("The result of String Concatnation using non parameterized joining :: ");
		System.out.println(concatedString);
		
		//String Concatenation using joining with delimiter parameter
		String delimitedString = streamSupplier.get().collect(Collectors.joining(","));
		System.out.println("The result of String Concatenation using joining with delimeter parameter :: ");
		System.out.println(delimitedString);
		
		//String Concatenation using joining with delimiter parameter
	    String concatString = streamSupplier.get().collect(Collectors.joining(",","[","]"));
	    System.out.println("The result of String Concatenation using joining with delimeter parameter :: ");
		System.out.println(concatString);
		
		//Collection Collectors
		List<String> listCollected =streamSupplier.get().collect(Collectors.toList());
		System.out.println("The list collected value of Stream are :: "+listCollected);
		
		Set<String> setCollected=streamSupplier.get().collect(Collectors.toSet());
		System.out.println("The set collected value of Stream are :: "+setCollected);
		
		Set<String> orderedSetCollected=streamSupplier.get().collect(Collectors.toCollection(TreeSet::new));
		System.out.println("The ordered set collected value of Stream are :: "+orderedSetCollected);
		
		//Map Collectors
		Map<String , Integer> mapCollected=orderedSetCollected.stream().collect(Collectors.toMap(x->x.toString(),x->x.toString().length() ));
		System.out.println("The generated Map values are :: "+mapCollected);
		
		//Map Collectors with duplicate key handling
		Map<String, List<Integer>>  mapWithDupVals=streamSupplier.get().collect(Collectors.toMap(x->x.toString(),                         //KeyMapper
				                            x -> {List <Integer> tmp = new ArrayList <> (); tmp.add(x.toString().length()); return tmp;}, //ValueMapper
				                           (L1, L2) -> { L1.addAll(L2); return L1;}                                                       //MergeFunction
				                           ));
       System.out.println("The generated Map values with duplicate values::"+mapWithDupVals);
       
       //Grouping Collectors
       Map<Integer,List<String>> groupExample= streamSupplier.get().collect(Collectors.groupingBy(x->x.toString().length()));
       System.out.println("Grouping stream elements on the basis of its length :: "+groupExample);
       
       //Partition Collectors       
       Map<Boolean,List<String>> partitionExample=streamSupplier.get().collect(Collectors.partitioningBy( x->x.toString().length() > 5 ));
       System.out.println("Patitioning of elements on the basis of its length :: "+partitionExample);
       
	}

}
