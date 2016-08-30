package com.storm.trident;

import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import com.storm.trident.function.MySplit;
import com.storm.trident.function.Split;

public class TridentPVTopo {

	public static StormTopology buildTopology(LocalDRPC drpc) {
		  Random random = new Random();
			String[] hosts = { "www.taobao.com" };
			String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
					"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678" };
			String[] time = { "2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-09 08:40:52", "2014-01-09 08:40:53", 
					"2014-01-07 09:40:49", "2014-01-08 10:40:49", "2014-01-08 11:40:49", "2014-01-09 12:40:49" };
		  
	     FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3, 
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]),
	    	new Values(hosts[0]+"\t"+session_id[random.nextInt(5)]+"\t"+time[random.nextInt(8)]));
	    
	    spout.setCycle(true);

	    TridentTopology topology = new TridentTopology();
	    TridentState wordCounts = topology.newStream("spout1", spout)
//	    	.parallelismHint(16)
	    	.each(new Fields("eachLog"),new MySplit("\t"), new Fields("date","session_id"))
	    	.groupBy(new Fields("date")).persistentAggregate(new MemoryMapState.Factory(),new Fields("session_id"),new Count(), new Fields("PV"));
//	        .parallelismHint(16);

	    topology.newDRPCStream("GetPV", drpc).each(new Fields("args"), new Split(" "), new Fields("date"))
	        .groupBy(new Fields("date"))
	        .stateQuery(wordCounts, new Fields("date"), new MapGet(), new Fields("PV"))
	        .each(new Fields("PV"),new FilterNull())
	        .applyAssembly(new FirstN(2, "PV", true));
//	        .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
	    return topology.build();
	  }
	
	public static void main(String[] args) {
		Config conf = new Config();
	    conf.setMaxSpoutPending(20);
	    if (args.length == 0) {
	      LocalDRPC drpc = new LocalDRPC();
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
	      for (int i = 0; i < 100; i++) {
	        System.err.println("DRPC RESULT: " + drpc.execute("GetPV", "2014-01-07 2014-01-08 2014-01-09 2014-01-10"));
	        Utils.sleep(1000);
	      }
	    }
	    else {
	      conf.setNumWorkers(3);
	      try {
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
		} catch (AlreadyAliveException | InvalidTopologyException
				| AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    }
	}
}
