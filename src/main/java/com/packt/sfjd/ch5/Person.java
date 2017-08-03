package com.packt.sfjd.ch5;

import java.io.Serializable;

public class Person implements Serializable{
	private String Name;
	private Integer Age;
	private String occupation;
	public String getOccupation() {
		return occupation;
	}
	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public Integer getAge() {
		return Age;
	}
	public void setAge(Integer age) {
		Age = age;
	}
}
