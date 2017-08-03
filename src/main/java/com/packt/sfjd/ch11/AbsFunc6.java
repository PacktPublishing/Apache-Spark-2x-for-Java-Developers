package com.packt.sfjd.ch11;

import java.io.Serializable;

import scala.Option;
import scala.runtime.AbstractFunction3;

public class AbsFunc6 extends AbstractFunction3<Object, String, Option<String>, String> implements Serializable {

	@Override
	public String apply(Object o, String s1, Option<String> s2) {
		
		if (s2.isEmpty()) {
			return s1 ;
		} else {
			return s1 + " " + s2.get();
		}

	}

}
