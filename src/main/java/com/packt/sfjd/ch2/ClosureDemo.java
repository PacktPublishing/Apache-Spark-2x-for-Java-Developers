package com.packt.sfjd.ch2;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ClosureDemo {
	public static void main(String[] args) {
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		Function<Integer, Integer> closure = ClosureExample.closure();
		list.stream().map(closure).forEach(n -> System.out.print(n+" "));
	}
}
