package com.storm.visit;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class DeepVisitBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_session_id","count"));
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
	Map<String, Integer> countMap;//同一天同一session id出现的次数
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String date = input.getStringByField("date");
		String sessionId = input.getStringByField("session_id");
		String key = date + "_" + sessionId;
		Integer count = countMap.get(key);
		if (count == null) {
			count = 0;
		}
		count++;
		countMap.put(key, count);
		collector.emit(new Values(key,count));
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		
	}

}
