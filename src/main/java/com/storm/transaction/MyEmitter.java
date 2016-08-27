package com.storm.transaction;

import java.math.BigInteger;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.tuple.Values;

public class MyEmitter implements ITransactionalSpout.Emitter<MyMata>{
	
	private Map<Long, String> dbMap;
	public MyEmitter(Map<Long, String> dbMap){
		this.dbMap = dbMap;
	}

	@Override
	public void cleanupBefore(BigInteger arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitBatch(TransactionAttempt tx, MyMata coordinateMata,
			BatchOutputCollector collector) {
		long beginPoint = coordinateMata.getBeginPoint();
		int num = coordinateMata.getNum();
		for (long i = beginPoint; i < beginPoint + num; i++) {
			if (dbMap.get(i) == null) {
				break;
			}
			collector.emit(new Values(tx, dbMap.get(i)));
		}
	}

}
