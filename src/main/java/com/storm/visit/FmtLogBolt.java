package com.storm.visit;

import java.util.Map;

import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.storm.tools.DateFmt;

public class FmtLogBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("date","session_id"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	String eachLog = "";
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		eachLog = input.getString(0);
		if (!Strings.isNullOrEmpty(eachLog)) {
			collector.emit(new Values(DateFmt.getCountDate(eachLog.split("\t")[2],DateFmt.date_short),
					eachLog.split("\t")[1])); //日期, session_id
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		
	}

}
