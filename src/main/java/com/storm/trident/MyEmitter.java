package com.storm.trident;

import java.util.Random;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout.Emitter;
import org.apache.storm.tuple.Values;

import com.storm.transaction.MyMata;

public class MyEmitter implements Emitter<MyMata>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void emitBatch(
			org.apache.storm.trident.topology.TransactionAttempt tx,
			MyMata coordinatorMeta, TridentCollector collector) {
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
		
		for (long i = 0; i < 100; i++) {
			collector.emit(new Values(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]));
		}
	}

	@Override
	public void success(
			org.apache.storm.trident.topology.TransactionAttempt arg0) {
		// TODO Auto-generated method stub
		
	}

	
}
