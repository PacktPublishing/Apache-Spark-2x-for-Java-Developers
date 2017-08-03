package com.packt.sfjd.ch10;

import java.io.Serializable;

public class Flight implements Serializable {
	/**
 |-- CRSArrTime: integer (nullable = true)
 |-- CRSDepTime: integer (nullable = true)
 |-- CRSElapsedTime: integer (nullable = true)
 |-- actualElapsedTime: integer (nullable = true)
 |-- airTime: integer (nullable = true)
 |-- arrDelay: double (nullable = true)
 |-- arrTime: integer (nullable = true)
 |-- dayOfWeek: string (nullable = true)
 |-- dayofMonth: string (nullable = true)
 |-- depDelay: integer (nullable = true)
 |-- depTime: integer (nullable = true)
 |-- distance: integer (nullable = true)
 |-- month: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- uniqueCarrier: string (nullable = true) 
	 */
	private static final long serialVersionUID = 1L;
	private String Month;
	private String DayofMonth;
	private String DayOfWeek;
	private Integer DepTime;
	private Integer CRSDepTime;
	private Integer ArrTime;
	private Integer CRSArrTime;	
	private String UniqueCarrier;
	private Integer ActualElapsedTime;
	private Integer CRSElapsedTime;
	private Integer AirTime;
	private Double ArrDelay;
	private Integer DepDelay;	
	private String Origin;
	private Integer Distance;
	
		
	
	public Flight(String month, String dayofMonth, String dayOfWeek,
			Integer depTime, Integer cRSDepTime, Integer arrTime,
			Integer cRSArrTime, String uniqueCarrier,
			Integer actualElapsedTime, Integer cRSElapsedTime, Integer airTime,
			Double arrDelay, Integer depDelay, String origin, Integer distance) {
		super();
		Month = month;
		DayofMonth = dayofMonth;
		DayOfWeek = dayOfWeek;
		DepTime = depTime;
		CRSDepTime = cRSDepTime;
		ArrTime = arrTime;
		CRSArrTime = cRSArrTime;
		UniqueCarrier = uniqueCarrier;
		ActualElapsedTime = actualElapsedTime;
		CRSElapsedTime = cRSElapsedTime;
		AirTime = airTime;
		ArrDelay = arrDelay;
		DepDelay = depDelay;
		Origin = origin;
		Distance = distance;
	}


	@Override
	public String toString() {
		return "Flight [Month=" + Month + ", DayofMonth=" + DayofMonth
				+ ", DayOfWeek=" + DayOfWeek + ", DepTime=" + DepTime
				+ ", CRSDepTime=" + CRSDepTime + ", ArrTime=" + ArrTime
				+ ", CRSArrTime=" + CRSArrTime + ", UniqueCarrier="
				+ UniqueCarrier + ", ActualElapsedTime=" + ActualElapsedTime
				+ ", CRSElapsedTime=" + CRSElapsedTime + ", AirTime=" + AirTime
				+ ", ArrDelay=" + ArrDelay + ", DepDelay=" + DepDelay
				+ ", Origin=" + Origin + ", Distance=" + Distance + "]";
	}
	
	
	public String getMonth() {
		return Month;
	}
	public void setMonth(String month) {
		Month = month;
	}
	public String getDayofMonth() {
		return DayofMonth;
	}
	public void setDayofMonth(String dayofMonth) {
		DayofMonth = dayofMonth;
	}
	public String getDayOfWeek() {
		return DayOfWeek;
	}
	public void setDayOfWeek(String dayOfWeek) {
		DayOfWeek = dayOfWeek;
	}
	public Integer getDepTime() {
		return DepTime;
	}
	public void setDepTime(Integer depTime) {
		DepTime = depTime;
	}
	public Integer getCRSDepTime() {
		return CRSDepTime;
	}
	public void setCRSDepTime(Integer cRSDepTime) {
		CRSDepTime = cRSDepTime;
	}
	public Integer getArrTime() {
		return ArrTime;
	}
	public void setArrTime(Integer arrTime) {
		ArrTime = arrTime;
	}
	public Integer getCRSArrTime() {
		return CRSArrTime;
	}
	public void setCRSArrTime(Integer cRSArrTime) {
		CRSArrTime = cRSArrTime;
	}
	public String getUniqueCarrier() {
		return UniqueCarrier;
	}
	public void setUniqueCarrier(String uniqueCarrier) {
		UniqueCarrier = uniqueCarrier;
	}
	public Integer getActualElapsedTime() {
		return ActualElapsedTime;
	}
	public void setActualElapsedTime(Integer actualElapsedTime) {
		ActualElapsedTime = actualElapsedTime;
	}
	public Integer getCRSElapsedTime() {
		return CRSElapsedTime;
	}
	public void setCRSElapsedTime(Integer cRSElapsedTime) {
		CRSElapsedTime = cRSElapsedTime;
	}
	public Integer getAirTime() {
		return AirTime;
	}
	public void setAirTime(Integer airTime) {
		AirTime = airTime;
	}
	public Double getArrDelay() {
		return ArrDelay;
	}
	public void setArrDelay(Double arrDelay) {
		ArrDelay = arrDelay;
	}
	public Integer getDepDelay() {
		return DepDelay;
	}
	public void setDepDelay(Integer depDelay) {
		DepDelay = depDelay;
	}
	public String getOrigin() {
		return Origin;
	}
	public void setOrigin(String origin) {
		Origin = origin;
	}
	public Integer getDistance() {
		return Distance;
	}
	public void setDistance(Integer distance) {
		Distance = distance;
	}
	

}
