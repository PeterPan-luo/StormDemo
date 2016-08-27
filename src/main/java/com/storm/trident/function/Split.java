package com.storm.trident.function;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Split extends BaseFunction{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String pattern;
	
	public Split(String pattern) {
		this.pattern = pattern;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence = tuple.getString(0);
		String[] words = sentence.split(pattern);
		if (words!= null && words.length > 0) {
			for(String word : words){
				collector.emit(new Values(word));
			}
		}
	}

}
