package com.storm.visit;

import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SourceSpout implements IRichSpout {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Queue<String> queue = new ConcurrentLinkedQueue<String>();
	private SpoutOutputCollector collector;

	
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nextTuple() {
		if (queue.size() >= 0) {
			this.collector.emit(new Values(queue.poll()));
		}
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			Random random = new Random();
			String[] hosts = { "www.taobao.com" };
			String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
					"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
			String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-07 08:40:52", "2014-01-07 08:40:53", 
					"2014-01-07 09:40:49", "2014-01-07 10:40:49", "2014-01-07 11:40:49", "2014-01-07 12:40:49" };
			
			for (int i = 0; i < 100; i++) {
				queue.add(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
