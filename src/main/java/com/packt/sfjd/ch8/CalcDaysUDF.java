package com.packt.sfjd.ch8;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.spark.sql.api.java.UDF2;

public class CalcDaysUDF implements UDF2<String,String, Long> {
	private static final long serialVersionUID = 1L;
	@Override
	public Long call(String dateString,String format) throws Exception {
		SimpleDateFormat myFormat = new SimpleDateFormat(format);
		    Date date1 = myFormat.parse(dateString);
		    Date date2 = new Date();
		    long diff = date2.getTime() - date1.getTime();
			return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);		
	}
}
