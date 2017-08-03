package com.packt.sfjd.ch11;

import java.io.Serializable;


public class AbsFunc3 extends scala.runtime.AbstractFunction2<Object, String, String> implements Serializable{

	@Override
	public String apply(Object arg0, String arg1) {
		
		return "Vertex:"+arg1;
	}
	
}