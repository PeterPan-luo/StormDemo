package com.storm.partition;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.transactional.TransactionalTopologyBuilder;

public class MyDailyTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("ttbId","spoutid",new MyPtTxSpout(),1);
		builder.setBolt("bolt1", new MyDailyBatchBolt(),3).shuffleGrouping("spoutid");
		builder.setBolt("committer", new MyDailyCommitterBolt(),1).shuffleGrouping("bolt1") ;
		
		Config conf = new Config() ;
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				try {
					StormSubmitter.submitTopology(args[0], conf, builder.buildTopology());
				} catch (AuthorizationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.buildTopology());
		}
		
		
		
		
	}

}
