package com.storm.log.stats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.log.conf.KafkaConfigureAPI.KafkaParam;

public class KafkaSpout implements IRichSpout{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSpout.class);
	
	private SpoutOutputCollector collector;
	private String topic;
	private ConsumerConnector consumer;
	
	public KafkaSpout(String topic) {
		this.topic = topic;
	}
	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", KafkaParam.ZK_HOSTS);
		props.put("group.id", KafkaParam.GROUP_ID);
		props.put("zookeeper.session.timeout.ms", "40000");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		try {
			this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
			Map<String, Integer> topickMap = new HashMap<String, Integer>();
			topickMap.put(topic, new Integer(1));
			Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topickMap);
			KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while (it.hasNext()) {
				String value = new String(it.next().message());
				LOGGER.info("[ Consumer ] Message is : " + value);
				collector.emit(new Values(value), value);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Spout has error,msg is " + ex.getMessage());
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("KafkaSpout"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	

}
