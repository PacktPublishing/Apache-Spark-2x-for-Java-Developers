package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class MethodReferenceExample {

	public static boolean isOdd(Integer n) { return n % 2 != 0; };
	public static boolean isEven(Integer n) { return n % 2 == 0; };
	
	
	public static void main(String[] args) {
		Supplier<Stream<String>> streamSupplier =()->Stream.of( new String[]{"Stream","from","an","array","of","objects"} ) ;
		
		//1.Static Method Reference	
        IntStream.range(1, 8).filter(MethodReferenceExample::isOdd).forEach(x->System.out.println(x));
        
        //Instance Method Reference
         IntStream.range(1, 8).filter(x-> x%2==0).forEach(System.out::println);
        
        //Constructor Reference
        TreeSet<String> hset=  streamSupplier.get().collect(Collectors.toCollection(TreeSet::new));
      
          
      //4.	Instance method Reference of an arbitrary object of a particular type
       System.out.println(" The sum of lengths are ::"+ streamSupplier.get().map(x->x.length()).reduce(Integer::sum));       
      
       
       	}

		
}

