package com.storm.drpc;

import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;

public class MyDRPCclient {

	public static void main(String[] args) throws TTransportException {
		DRPCClient client = new DRPCClient(null, "192.168.1.107", 3772);
		try {
			String result = client.execute("exclamation", "hello ");
			
			System.out.println(result);
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
