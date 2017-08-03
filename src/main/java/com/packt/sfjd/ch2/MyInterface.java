package com.packt.sfjd.ch2;

public interface MyInterface {

	default String hello() {
		return "Inside static method in interface";
	}

	void absmethod();
}
