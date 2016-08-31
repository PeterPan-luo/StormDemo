package com.storm.hbase;

import hbase.state.Configuration;
import hbase.state.HTable;
import hbase.state.Path;
import hbase.state.TupleTableConfig;

import java.io.IOException;
import java.io.Serializable;

public class HTableConnector implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private Configuration configuration;
	protected HTable table;
	private String tableName;
	
	public HTableConnector(TupleTableConfig conf) throws Exception
	{
		this.tableName = conf.getTableName();
		this.configuration = HBaseConfiguration.create();
		
		String filePathString = "hbase-site.xml" ;
		Path path = new Path(filePathString) ;
		this.configuration.addResource(path);
		this.table = new HTable(this.configuration,this.tableName);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	public HTable getTable() {
		return table;
	}

	public void setTable(HTable table) {
		this.table = table;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public void close()
	{
		try {
			this.table.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
