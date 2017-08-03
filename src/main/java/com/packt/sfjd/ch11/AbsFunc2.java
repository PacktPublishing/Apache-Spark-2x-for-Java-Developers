package com.packt.sfjd.ch11;

import java.io.Serializable;

import scala.runtime.AbstractFunction2;

public class AbsFunc2 extends AbstractFunction2<Object, String, Object> implements Serializable{

	@Override
	public Object apply(Object arg0, String arg1) {
		
		return true;
	}
	
}