package com.packt.sfjd.ch2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

public class WordCountInJava {
	public static final String REGEX = "\\s+";
	public static final String NEW_LINE_CHAR = "\n";
	public static final String imagineLyrics="Imagine there's no heaven \n"
											+ "It's easy if you try \n"
											+ "No hell below us \n"
											+ "Above us only sky \n"
											+ "Imagine all the people living for today";

	public static void main(String[] args) {

		try {
		//TreeMap<String, Long> count = Files.lines(Paths.get(args[0]), StandardCharsets.UTF_8)
			TreeMap<String, Long> count = Stream.of( imagineLyrics.split(NEW_LINE_CHAR))
					.map(line -> line.split(REGEX))
					.flatMap(Arrays::stream)
					.collect(groupingBy(identity(), TreeMap::new, counting()));
			
			// Using Lambda Expression
			Stream.of(count).forEach(x -> System.out.println(x));
			//Using Method Reference
			Stream.of(count).forEach(System.out::println);
			
			
			Stream<String> mapResult=Stream.of( imagineLyrics.split(NEW_LINE_CHAR))
			.map(line -> line.split(REGEX)).flatMap(Arrays::stream);
			
			//sort and suffle phase			
			Map<String,List<Integer>> sortedData=mapResult.collect(Collectors.toMap(x->x.toString(), x->{
				List<Integer> temp=new ArrayList<>(); temp.add(1); return temp;	},
				(L1,L2)-> {L1.addAll(L2);return L1;}));
			//Reduce Phase
			/*Map<String,Integer> wordCount=sortedData.entrySet().stream().collect(Collectors.toMap(
		            e -> e.getKey(),
		            e -> Integer.parseInt(e.getValue())
		        ));*/
					
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
