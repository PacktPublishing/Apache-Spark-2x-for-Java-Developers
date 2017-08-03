package com.packt.sfjd.ch7;

import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.spark.util.AccumulatorV2;

public class ListAccumulator extends AccumulatorV2<String, CopyOnWriteArrayList<Integer>> {

	private static final long serialVersionUID = 1L;
	private CopyOnWriteArrayList<Integer> accList = null;

	public ListAccumulator() {
		accList = new CopyOnWriteArrayList<Integer>();

	}

	public ListAccumulator(CopyOnWriteArrayList<Integer> value) {
		if (value.size() != 0) {
			accList = new CopyOnWriteArrayList<Integer>(value);
		}
	}

	@Override
	public void add(String arg) {
		if (!arg.isEmpty())
			accList.add(Integer.parseInt(arg));

	}

	@Override
	public AccumulatorV2<String, CopyOnWriteArrayList<Integer>> copy() {
		return new ListAccumulator(value());
	}

	@Override
	public boolean isZero() {
		return accList.size() == 0 ? true : false;
	}

	@Override
	public void merge(AccumulatorV2<String, CopyOnWriteArrayList<Integer>> other) {
		add(other.value());

	}

	private void add(CopyOnWriteArrayList<Integer> value) {
		value().addAll(value);
	}

	@Override
	public void reset() {
		accList = new CopyOnWriteArrayList<Integer>();
	}

	@Override
	public CopyOnWriteArrayList<Integer> value() {
		return accList;
	}

}
