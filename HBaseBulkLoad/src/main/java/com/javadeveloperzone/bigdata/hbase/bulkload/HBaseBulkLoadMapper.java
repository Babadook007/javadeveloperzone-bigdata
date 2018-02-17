package com.javadeveloperzone.bigdata.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> 
{
	
	private String hbaseTable; 
	
	private String dataSeperator;
	
	private String columnFamily1;
	
	private String columnFamily2;
	
	private ImmutableBytesWritable hbaseTableName;
	
	public void setup(Context context) 
	{
		
		Configuration configuration = context.getConfiguration();
		
		hbaseTable = configuration.get("hbase.table.name");
		
		dataSeperator = configuration.get("data.seperator");
		
		columnFamily1 = configuration.get("COLUMN_FAMILY_1");
		
		columnFamily2 = configuration.get("COLUMN_FAMILY_2");
		
		hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
		
	}
	
	public void map(LongWritable key, Text value, Context context)
	{
		try
		{
		
			String[] values = value.toString().split(dataSeperator);
			
			String rowKey = values[0];
			
			Put put = new Put(Bytes.toBytes(rowKey));
			
			put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("movie_name"), Bytes.toBytes(values[6]));
			
			put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("movie_genre"), Bytes.toBytes(values[7]));
			
			put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("actor"), Bytes.toBytes(values[8]));
			
			put.add(Bytes.toBytes(columnFamily1), Bytes.toBytes("release_date"), Bytes.toBytes(values[16]));
			
			context.write(hbaseTableName, put);
			
		}
		catch(Exception exception)
		{
			
			exception.printStackTrace();
			
		}
		
	}

}
