package com.storm.visit;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.shade.com.google.common.base.Strings;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.storm.tools.DateFmt;

public class UVSumBolt implements IBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String cur_date = null;
	
	private Map<String, Integer> counts = new HashMap<String, Integer>();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			long PV = 0;// 总数
			long UV = 0; // 个数，去重后
			String dateSession_id = input.getString(0);
			Integer count = input.getInteger(1);

			if (!dateSession_id.startsWith(cur_date)
					&& DateFmt.parseDate(dateSession_id.split("_")[0]).after(
							DateFmt.parseDate(cur_date))) {
				cur_date = dateSession_id.split("_")[0];
				counts.clear();
			}

			counts.put(dateSession_id, count);
			Iterator<String> keIterator = counts.keySet().iterator();
			while (keIterator.hasNext()) {
				String dateSessionID = (String) keIterator.next();
				if (!Strings.isNullOrEmpty(dateSessionID)) {
					if (dateSession_id.startsWith(cur_date)) {
						UV++;
						PV += counts.get(dateSessionID);
					}
				}
			}
			System.err.println("PV=" + PV + ";  UV="+ UV);

		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		cur_date = DateFmt.getCountDate("2014-01-07", DateFmt.date_short);		
	}

}
