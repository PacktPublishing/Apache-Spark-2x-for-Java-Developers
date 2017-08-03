package com.packt.sfjd.ch2.generics;

import java.util.ArrayList;
import java.util.List;

public class FirstExample {
public static void main(String[] args) {
	
	List<Integer> list1 =new ArrayList<Integer>();
	List<String> list2 =new ArrayList<String>();
	
	List list =new ArrayList<>();
	
	list.add(1);
	list.add(2);
	list.add("hello");
	
	Integer object = (Integer)list.get(0);
	
	System.out.println(object);
	
	List<Integer> listGeneric =new ArrayList<>();
	
	listGeneric.add(1);
	listGeneric.add(2);
	//list1.add("hello"); - wont work
	Integer intObject = listGeneric.get(0);
}
}
