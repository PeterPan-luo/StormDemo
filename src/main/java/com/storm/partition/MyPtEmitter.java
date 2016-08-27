package com.storm.partition;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import org.apache.storm.tuple.Values;

import com.storm.transaction.MyMata;

public class MyPtEmitter implements IPartitionedTransactionalSpout.Emitter<MyMata>{

	public static int BATCH_NUM = 5;
	public Map<Integer, Map<Long,String>> PT_DATA_MP = new HashMap<Integer, Map<Long,String>>();
	
	public MyPtEmitter(Map<Integer, Map<Long,String>> dataMap) {
		this.PT_DATA_MP = dataMap;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitPartitionBatch(TransactionAttempt tx,
			BatchOutputCollector collector, int partition, MyMata partitionMeta) {
		
		System.err.println("emitPartitionBatch partition:"+partition);
		long beginPoint = partitionMeta.getBeginPoint() ;
		int num = partitionMeta.getNum() ;
		
		Map<Long, String> batchMap = PT_DATA_MP.get(partition);
		for (long i = beginPoint; i < num+beginPoint; i++) {
			if (batchMap.get(i)==null) {
				break;
			}
			collector.emit(new Values(tx,batchMap.get(i)));
		}
	}

	@Override
	public MyMata emitPartitionBatchNew(TransactionAttempt tx,
			BatchOutputCollector collector, int partition, MyMata lastPartitionMeta) {
		long beginPoint = 0;
		if (lastPartitionMeta == null) {
			beginPoint = 0 ;
		}else {
			beginPoint = lastPartitionMeta.getBeginPoint() + lastPartitionMeta.getNum() ;
		}
		
		MyMata mata = new MyMata() ;
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		
		emitPartitionBatch(tx,collector,partition,mata);
		
		System.err.println("启动一个事务："+mata.toString());
		return mata;
	}

}
