package com.packt.sfjd.ch11;

import java.io.Serializable;


public class AbsFunc5 extends scala.runtime.AbstractFunction2<Integer, Integer, Integer> implements Serializable{

	@Override
	public Integer apply(Integer i1, Integer i2) {
		
		return i1+i2;
	}
	
}