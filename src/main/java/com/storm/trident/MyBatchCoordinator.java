package com.storm.trident;

import org.apache.storm.trident.spout.ITridentSpout.BatchCoordinator;

import com.storm.transaction.MyMata;

public class MyBatchCoordinator implements BatchCoordinator<MyMata>{

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MyMata initializeTransaction(long arg0, MyMata arg1, MyMata arg2) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReady(long arg0) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void success(long arg0) {
		// TODO Auto-generated method stub
		
	}

}
