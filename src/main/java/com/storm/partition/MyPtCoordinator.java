package com.storm.partition;

import org.apache.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import org.apache.storm.utils.Utils;

public class MyPtCoordinator implements IPartitionedTransactionalSpout.Coordinator{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isReady() {
		Utils.sleep(5000);
		return true;
	}

	@Override
	public int numPartitions() {
		return 5;
	}

}
