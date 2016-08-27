package com.storm.visit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class UVTopo {
	
	public static void main(String[] args) {
		TopologyBuilder topoBuilder = new TopologyBuilder();
		topoBuilder.setSpout("spout", new SourceSpout(),1);
		topoBuilder.setBolt("fmtBolt", new FmtLogBolt(),4).shuffleGrouping("spout");
		//同一天，同一个session_id的一组
		topoBuilder.setBolt("deepVisitBolt", new DeepVisitBolt(),4).fieldsGrouping("fmtBolt", new Fields("date","session_id"));
		topoBuilder.setBolt("UvSum", new UVSumBolt(),1).shuffleGrouping("deepVisitBolt");
		
		Config config = new Config();
		config.setDebug(Boolean.TRUE);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], config, topoBuilder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException
					| AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", config, topoBuilder.createTopology());
		}
	}
}
