package com.packt.sfjd.ch2.generics;

public class MyGenericsDemo {

	public static void main(String[] args) {
		MyGeneric<Integer> m1 =new MyGeneric<Integer>(1);
		System.out.println(m1.getInput());
		
		MyGeneric<String> m2 =new MyGeneric<String>("hello");
		System.out.println(m2.getInput());
	}
}
