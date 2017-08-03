package com.packt.sfjd.ch4.transformations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestMain {
public static void main(String[] args) {
	List<Test> list =new ArrayList<>();
	list.add(new Test(5));
	list.add(new Test(3));
	list.add(new Test(6));
	
	//Collections.sort(list);
	list.forEach(t -> System.out.println(t.getAge()));
	
}
}
