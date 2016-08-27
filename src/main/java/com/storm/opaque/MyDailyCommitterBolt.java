package com.storm.opaque;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Tuple;

public class MyDailyCommitterBolt extends BaseTransactionalBolt implements ICommitter{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static Map<String, DbValue> dbMap = new HashMap<String, DbValue>() ;
	
	Map<String, Integer> countMap = new HashMap<String, Integer>();
	TransactionAttempt id ;
	BatchOutputCollector collector;
	String today = null;
	@Override
	public void execute(Tuple tuple) {
		today = tuple.getString(1) ;
		Integer count = tuple.getInteger(2);
		id = (TransactionAttempt)tuple.getValue(0);
		
		if (today !=null && count != null) {
			Integer batchCount = countMap.get(today) ;
			if (batchCount == null) {
				batchCount = 0;
			}
			batchCount += count ;
			countMap.put(today, batchCount);
		}
	}

	@Override
	public void finishBatch() {
		// TODO Auto-generated method stub
		if (countMap.size() > 0) {
			DbValue value = dbMap.get(today);
			DbValue newValue ;
			if (value == null || !value.txid.equals(id.getTransactionId())) {
				//更新数据库
				newValue = new DbValue();
				newValue.txid = id.getTransactionId() ;
				newValue.dateStr = today;
				if (value == null) {
					newValue.count = countMap.get(today) ;
					newValue.pre_count = 0;
				}else {
					newValue.pre_count = value.count ;
					newValue.count = value.count + countMap.get("2014-01-07") ;
				}
				dbMap.put(today, newValue);
			}else
			{
				newValue = value;
			}
			System.out.println("total==========================:"+dbMap.get(today).count);
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		// TODO Auto-generated method stub
		this.id = id ;
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	public static class DbValue
	{
		BigInteger txid;
		int count = 0;
		String dateStr;
		int pre_count;
	}

}
