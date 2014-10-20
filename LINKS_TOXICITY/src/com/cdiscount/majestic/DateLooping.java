package com.cdiscount.majestic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateLooping {	
	public static void main(String[] args){
		long oneDayMilSec = 86400000; // number of milliseconds in one day
		long twoWeeksMilSec = 14*oneDayMilSec; // number of milliseconds in one day
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try {
		    Date startDate = sdf.parse("2013-01-01");
		    Date endDate = sdf.parse("2014-09-01");

		    long startDateMilSec = startDate.getTime();
		    long endDateMilSec = endDate.getTime();

		    for(long d=startDateMilSec; d<=endDateMilSec; d=d+twoWeeksMilSec){
		    	Date localDate = new Date(d);
		    	System.out.println(sdf.format(localDate));
		    }
		} catch (ParseException e) {
		    e.printStackTrace();
		}
	}
}
