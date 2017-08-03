package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.List;

public class LambdaExamples {
public static void main(String[] args) {
	List<Integer> list = Arrays.asList(1,2,3,4,5);
	
	list.forEach(n-> System.out.println(n));
	
	list.stream().map(n -> n*2 ).forEach(n-> System.out.println(n));;
	list.stream().map(n->{
		return n*2;
	}).forEach(System.out::println);
	
}





}
