package com.storm.opaque;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Values;

import com.storm.transaction.MyMata;

public class MyOpaqueEmitter implements IOpaquePartitionedTransactionalSpout.Emitter<MyMata>{

	public static int BATCH_NUM = 10 ;
	public Map<Integer, Map<Long,String>> PT_DATA_MP = new HashMap<Integer, Map<Long,String>>();
	
	public MyOpaqueEmitter(Map<Integer, Map<Long,String>> dataMap) {
		this.PT_DATA_MP = dataMap;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MyMata emitPartitionBatch(TransactionAttempt tx,
			BatchOutputCollector collector, int partition, MyMata lastPartitionMeta) {
		System.err.println("emitPartitionBatch partition:"+partition);
		long beginPoint = 0;
		if (lastPartitionMeta == null) {
			beginPoint = 0 ;
		}else {
			beginPoint = lastPartitionMeta.getBeginPoint() + lastPartitionMeta.getNum() ;
		}
		
		MyMata mata = new MyMata() ;
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		System.err.println("启动一个事务："+mata.toString());
//		emitPartitionBatch(tx,collector,partition,mata);
		Map<Long, String> batchMap = PT_DATA_MP.get(partition);
		for (Long i = mata.getBeginPoint(); i < mata.getBeginPoint()+mata.getNum(); i++) {
			if (batchMap.size()<=i) {
				break;
			}
			collector.emit(new Values(tx,batchMap.get(i)));
		}
		return mata;
	}

	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return 5;
	}

}
