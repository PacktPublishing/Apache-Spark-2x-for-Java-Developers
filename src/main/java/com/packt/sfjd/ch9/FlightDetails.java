package com.packt.sfjd.ch9;

import java.io.Serializable;

public class FlightDetails implements Serializable {
	private String flightId;
	private double temperature;
	private boolean landed;
	private long timestamp;

	public String getFlightId() {
		return flightId;
	}

	public void setFlightId(String flightId) {
		this.flightId = flightId;
	}

	public double getTemperature() {
		return temperature;
	}

	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}

	public boolean isLanded() {
		return landed;
	}

	public void setLanded(boolean landed) {
		this.landed = landed;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "FlightDetails [flightId=" + flightId + ", temperature=" + temperature + ", landed=" + landed
				+ ", timestamp=" + timestamp + "]";
	}

	public static void main(String[] args) {
		int x=1;
		int x1=1;
		int y =x++;
	   int z  = x1 + x1++;
		System.out.println(y+""+z);
	}
}
