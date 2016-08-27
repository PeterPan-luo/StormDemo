package com.storm.trident;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;

import com.storm.transaction.MyMata;

public class ITridentSpout implements org.apache.storm.trident.spout.ITridentSpout<MyMata>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator<MyMata> getCoordinator(
			String arg0, Map arg1, TopologyContext arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public org.apache.storm.trident.spout.ITridentSpout.Emitter<MyMata> getEmitter(
			String arg0, Map arg1, TopologyContext arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

}
