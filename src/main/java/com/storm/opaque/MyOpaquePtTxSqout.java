package com.storm.opaque;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.tuple.Fields;

public class MyOpaquePtTxSqout implements IOpaquePartitionedTransactionalSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static int BATCH_NUM = 10 ;
	public Map<Integer, Map<Long,String>> PT_DATA_MP = new HashMap<Integer, Map<Long,String>>();
	
	public MyOpaquePtTxSqout() {
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
		String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
				"2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
		
		for (int j = 0; j < 5; j++) {
			HashMap<Long, String> dbMap = new HashMap<Long, String> ();
			for (long i = 0; i < 100; i++) {
				dbMap.put(i,hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]);
			}
			PT_DATA_MP.put(j, dbMap);
		}
	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tx","log"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Coordinator getCoordinator(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		return new MyOpaqueCoordinator();
	}

	@Override
	public Emitter getEmitter(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		return new MyOpaqueEmitter(PT_DATA_MP);
	}

}
