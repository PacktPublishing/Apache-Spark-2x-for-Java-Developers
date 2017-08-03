package com.packt.sfjd.ch4.transformations;

import java.io.Serializable;

public class Test implements Serializable{//implements Comparable<Test>,Serializable{

	Test(int age)
	{
		this.age=age;
	}
	private int age;
	
	
	public int getAge() {
		return age;
	}


	public void setAge(int age) {
		this.age = age;
	}


//	@Override
//	public int compareTo(Test o) {
//		
//		return this.getAge()-o.getAge();
//	}


	@Override
	public String toString() {
		return "Test [age=" + age + "]";
	}
	
	

}
