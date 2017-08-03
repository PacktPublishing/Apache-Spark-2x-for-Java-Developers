package com.packt.sfjd.ch2.generics;

public class MyGeneric<T> {
	
	T input;
	
	public MyGeneric(T input) {
		this.input=input;
			
	}
	
	public T getInput()
	{
		return input;
	}
	
	
}
