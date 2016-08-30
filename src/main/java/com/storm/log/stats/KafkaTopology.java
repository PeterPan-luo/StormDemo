package com.storm.log.stats;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class KafkaTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafkaUbasGroup",  new KafkaSpout("kafka-ubas"));
		builder.setBolt("messageBlots", new MessageBlots()).shuffleGrouping("kafkaUbasGroup");
		builder.setBolt("kpiCounter", new StatsBlots(), 2).fieldsGrouping("messageBlots", new Fields("attribute"));
		Config config = new Config();
		// config.setDebug(true);
		// storm.messaging.netty.max_retries
		// storm.messaging.netty.max_wait_ms
		if (args != null && args.length > 0) {
			// online commit Topology
			config.put(Config.NIMBUS_HOST, args[0]);
			config.setNumWorkers(3);
			try {
				StormSubmitter.submitTopologyWithProgressBar(KafkaTopology.class.getSimpleName(), config,
						builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// Local commit jar
			LocalCluster local = new LocalCluster();
			local.submitTopology("stats", config, builder.createTopology());
//			try {
//				Thread.sleep(50);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			// local.shutdown();
		}
	}

}
