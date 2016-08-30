package com.storm.log.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MessageBlots implements IRichBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		String[] line = input.getString(0).split(",");
		for (int i = 0; i < line.length; i++) {
			List<Tuple> a = new ArrayList<Tuple>();
			a.add(input);
			switch (i) {
			case 0:
				this.collector.emit(a, new Values(line[i]));
				break;
			case 3:
				this.collector.emit(a, new Values(line[i]));
				break;
			case 4:
				this.collector.emit(a, new Values(line[i]));
				break;
			case 6:
				this.collector.emit(a, new Values(line[i]));
				break;
			default:
				break;
 			}
		}
		this.collector.ack(input);
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("attribute"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}


}
