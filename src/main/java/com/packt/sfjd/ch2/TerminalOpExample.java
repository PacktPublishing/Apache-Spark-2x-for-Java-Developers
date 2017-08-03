package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TerminalOpExample {

	public static void main(String[] args) {
		// forEach
		Supplier<Stream<String>> streamSupplier =()->Stream.of( new String[]{"Stream","from","an","array","of","objects"} ) ;
		//Sequential For each
		streamSupplier.get().sequential().forEach(P->System.out.println("Sequential output :: "+P));
		//Parallel For each
		streamSupplier.get().parallel().forEach(P->System.out.println("Parallel output :: "+P));
		
	//sum
	//	System.out.println(streamSupplier.get().map(x -> x.toString().length()).peek(System.out::println).sum());
		
		System.out.println("Number of alphabets present in the stream ::"+streamSupplier.get().mapToInt(x -> x.length()).sum());   //Notice here had we used MAP , we would have had to another map function to convert the in Int.
		 
    //reduce
		Optional<Integer> simpleSum= streamSupplier.get().map(x->x.length()).reduce((x,y)-> x+y);
		
		System.out.println( "The value with simpe reduce is ::"+simpleSum.get());
		
		Integer defaulValSum= streamSupplier.get().map(x->x.length()).reduce(0,(x,y)-> x+y);
		System.out.println( "The value with default reduce is ::"+defaulValSum);
		
		Integer valSum= streamSupplier.get().reduce(0,(x,y)-> x+y.length(),(acc1,acc2)->acc1+acc2);
		System.out.println("The value with with cobine reduce is ::"+valSum);
		
   //collect		
	
		StringBuilder concat = streamSupplier.get()
                .collect(() -> new StringBuilder(),
                         (sbuilder, str) -> sbuilder.append(str),
                         (sbuilder1, sbuiler2) -> sbuilder1.append(sbuiler2));
		
		
		StringBuilder concatM = streamSupplier.get()
                .collect(StringBuilder::new,
                         StringBuilder::append,
                         StringBuilder::append);
		
		String concatC = streamSupplier.get().collect(Collectors.joining());
	
		//Match
	boolean matchesAll =streamSupplier.get().allMatch(x->x.toString().length() > 1);
	System.out.println("All the elemetns have lenght greater than 1 ::"+matchesAll);	
	
	boolean noneMatches =streamSupplier.get().noneMatch(x->x.toString().length() > 1);
	System.out.println("None of the elemetns have lenght greater than 1 ::"+noneMatches);	
	
	boolean anyMatches =streamSupplier.get().peek(x->System.out.println("Element being iterated is :: "+x)).anyMatch(x->x.toString().length() == 2);
	System.out.println("The short circuit terminal operation finished with return value :: "+anyMatches);
	
	//Finding Element
	System.out.println("In a paralled stream from 5-100 finding any element :: "+IntStream.range(5, 100).parallel().findAny());
	
	System.out.println("In a paralled stream from 8-100 finding the first element :: "+IntStream.range(8, 100).parallel().findFirst());
	
	//Count 		
	long elementCount=streamSupplier.get().count();
	System.out.println("The number of elements in the stream are :: "+elementCount);
	
	
	
	//System.out.println( joinWithReduce(Stream . of ( "foo" , "bar" , "baz" ) ));
	
	//System.out.println( joinWithCollect(Stream . of ( "foo" , "bar" , "baz" ) ));

	}
	
	static String joinWithReduce ( Stream < String > stream ) { // BAD 
		return stream.reduce( new StringBuilder (), StringBuilder :: append , StringBuilder :: append ). toString (); } 
	
	static String joinWithCollect ( Stream < String > stream ) { // OK 
		return stream.collect ( StringBuilder :: new , StringBuilder :: append , StringBuilder :: append ). toString (); }
	

}
