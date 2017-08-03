package com.packt.sfjd.ch8;

import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

public class TypeSafeUDAF extends Aggregator<Employee, Average, Double> implements Serializable{
	private static final long serialVersionUID = 1L;

	public Average zero() {
		return new Average(0L, 0L);
	}

	public Average reduce(Average buffer, Employee employee) {
		double newSum = buffer.getSumVal() + employee.getSalary();
		long newCount = buffer.getCountVal() + 1;
		buffer.setSumVal(newSum);
		buffer.setCountVal(newCount);
		return buffer;
	}

	public Average merge(Average b1, Average b2) {
		double mergedSum = b1.getSumVal() + b2.getSumVal();
		long mergedCount = b1.getCountVal() + b2.getCountVal();
		b1.setSumVal(mergedSum);
		b1.setCountVal(mergedCount);
		return b1;
	}

	public Double finish(Average reduction) {
		return ((double) reduction.getSumVal()) / reduction.getCountVal();
	}

	public Encoder<Average> bufferEncoder() {
		return Encoders.bean(Average.class);
	}

	public Encoder<Double> outputEncoder() {
		return Encoders.DOUBLE();
	}
}
