package com.storm.hbase;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;

public class HBaseAggregateFactory implements StateFactory{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private StateType type;
	private TridentConfig config;
	
	public HBaseAggregateFactory(TridentConfig config,StateType stateType){
		this.type = stateType;
		this.config = config;
		if (config.getStateSerializer() == null) {
			config.setStateSerializer(TridentConfig.DEFAULT_SERIALIZERS.get(type));
		}
	}
	
	@Override
	public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
		HBaseAggregateState state = new HBaseAggregateState(config);
		CachedMap c = new CachedMap(state, config.getStateCacheSize());
		MapState ms = null;
		if (type == StateType.NON_TRANSACTIONAL) {
			ms = NonTransactionalMap.build(c);
			
		}else if (type == StateType.OPAQUE) {
			ms = OpaqueMap.build(c);
		}
		else if(type == StateType.TRANSACTIONAL){
			ms = TransactionalMap.build(c);
		}
		return new SnapshottableMap(ms, new Values("$GLOBAL$"));
	}

}
