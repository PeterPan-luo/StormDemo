package com.storm.partition;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.IBatchBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.storm.tools.DateFmt;

public class MyDailyBatchBolt implements IBatchBolt<TransactionAttempt>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	Map<String, Integer> countMap = new HashMap<String, Integer>();
	BatchOutputCollector collector ;
	Integer count = null;
	String today = null;
	TransactionAttempt tx = null;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","date","count"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(Tuple input) {
		String log = input.getString(1);
		tx = (TransactionAttempt)input.getValue(0);
		if (log != null && log.split("\\t").length >=3 ) {
			today = DateFmt.getCountDate(log.split("\\t")[2], DateFmt.date_short) ;
			count = countMap.get(today);
			if(count == null)
			{
				count = 0;
			}
			count ++ ;
			
			countMap.put(today, count);
		}
	}

	@Override
	public void finishBatch() {
		System.err.println(tx+"--"+today+"--"+count);
		collector.emit(new Values(tx,today,count));
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt tx) {
		this.collector = collector;
	}

}
