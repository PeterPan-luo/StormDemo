package com.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ManualDRPC {
	
	public static class ExclamationBolt extends BaseBasicBolt{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String arg = input.getString(0);
		    Object retInfo = input.getValue(1);
		    collector.emit(new Values(arg + "!!!", retInfo));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("result", "return-info"));
		}
		
	}
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
	    LocalDRPC drpc = new LocalDRPC();

	    if (args.length > 0) {
	    	DRPCSpout spout = new DRPCSpout("exclamation");
    	    builder.setSpout("drpc", spout);
    	    builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
    	    builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

    	    Config conf = new Config();
    	    try {
				try {
					StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
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
			DRPCSpout spout = new DRPCSpout("exclamation", drpc);
		    builder.setSpout("drpc", spout);
		    builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
		    builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

		    LocalCluster cluster = new LocalCluster();
		    Config conf = new Config();
		    cluster.submitTopology("exclaim", conf, builder.createTopology());
		}
	}

}
