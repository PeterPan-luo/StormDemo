package com.storm.transaction;

import java.math.BigInteger;

import org.apache.storm.transactional.ITransactionalSpout.Coordinator;
import org.apache.storm.utils.Utils;

public class MyCoordinator implements Coordinator<MyMata> {

	public static int BATCH_NUM = 10 ;

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MyMata initializeTransaction(BigInteger txid, MyMata preMata) {
		long beginPoint = 0;
		if (preMata != null) {
			beginPoint = preMata.getBeginPoint() + preMata.getNum();
		}
		MyMata mata = new MyMata();
		mata.setBeginPoint(beginPoint);
		mata.setNum(BATCH_NUM);
		System.out.println("开启一个新的事务...");
		return mata;
	}

	@Override
	public boolean isReady() {
		Utils.sleep(2000);
		return true;
	}

}
