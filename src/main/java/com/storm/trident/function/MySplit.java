package com.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import com.storm.tools.DateFmt;

public class MySplit extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String pattern;
	
	public MySplit(String pattern) {
		this.pattern = pattern;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String log = tuple.getString(0);
	    String logArr[] = log.split(pattern);
	    if (logArr.length == 3) {
	  	  collector.emit(new Values(DateFmt.getCountDate(logArr[2], DateFmt.date_short),"cf","pv_count",logArr[1]));
	    }
	}

}
