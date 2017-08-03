package com.packt.sfjd.ch11;

import java.io.Serializable;

import org.apache.spark.graphx.EdgeTriplet;

import scala.runtime.AbstractFunction1;

public class AbsFunc1 extends AbstractFunction1<EdgeTriplet<String,String>, Object> implements Serializable{

	
	@Override
	public Object apply(EdgeTriplet<String, String> arg0) {
		return arg0.attr().equals("Friend");
	}
	
}