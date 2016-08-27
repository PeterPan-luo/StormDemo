package com.storm.transaction;

import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MyTransactionBolt extends BaseTransactionalBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private BatchOutputCollector collector;
	TransactionAttempt tx ;
	int count = 0;

	@Override
	public void execute(Tuple  input) {
		tx = (TransactionAttempt) input.getValue(0);
		System.err.println("MyTransactionBolt TransactionAttempt "+tx.getTransactionId() +"  attemptid"+tx.getAttemptId());
		String log = input.getString(1);
		if (!Strings.isNullOrEmpty(log)) {
			count++;
		}
	}

	@Override
	public void finishBatch() {
		collector.emit(new Values(tx, count));
		System.err.println("finishBatch "+count );

	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			BatchOutputCollector collector, TransactionAttempt transactionAttempt) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","count"));
	}

}
