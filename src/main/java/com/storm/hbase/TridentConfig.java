package com.storm.hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateType;

public class TridentConfig<T> extends TupleTableConfig {

	public TridentConfig(String table, String rowkeyField) {
		super(table, rowkeyField);
	}
	
	public TridentConfig(String table, String rowkeyField, String tupleTimestampField) {
		super(table, rowkeyField,tupleTimestampField);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int stateCacheSize = 1000;
	private Serializer<T> stateSerializer;
	public int getStateCacheSize() {
		return stateCacheSize;
	}

	public void setStateCacheSize(int stateCacheSize) {
		this.stateCacheSize = stateCacheSize;
	}

	public Serializer<T> getStateSerializer() {
		return stateSerializer;
	}

	public void setStateSerializer(Serializer<T> stateSerializer) {
		this.stateSerializer = stateSerializer;
	}
	
	public static final Map<StateType, Serializer> DEFAULT_SERIALIZERS = new HashMap<StateType, Serializer>(){
		{
			put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
			put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
			put(StateType.OPAQUE, new JSONOpaqueSerializer());
		}
	};

}
