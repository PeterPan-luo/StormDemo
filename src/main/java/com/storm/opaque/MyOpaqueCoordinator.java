package com.storm.opaque;

import org.apache.storm.transactional.partitioned.IOpaquePartitionedTransactionalSpout;
import org.apache.storm.utils.Utils;

public class MyOpaqueCoordinator implements IOpaquePartitionedTransactionalSpout.Coordinator{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
